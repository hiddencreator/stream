import asyncio
import html
import json
import os
import re
import secrets
import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, Response, StreamingResponse
from pyrogram import Client, enums, filters
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
    User,
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "6"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")
ENV_OWNER_ID = int(os.getenv("TELEGRAM_OWNER_ID", "0") or 0)
DEFAULT_SUNTV_URL = "https://d2px1d6ssc60fw.cloudfront.net/SunTV/SunTV-HDR10-IN-index.m3u8"

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = Path("/tmp/yplay_chunks")
HLS_DIR = Path("/dev/shm/suntv_hls")
LIBRARY_FILE = DATA_DIR / "yplay_library.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
OWNER_FILE = DATA_DIR / "owner.json"
STATE_FILE = DATA_DIR / "system_state.json"
DEVICES_FILE = DATA_DIR / "devices.json"
SESSION_FILE = DATA_DIR / "ystream_bot"

CHUNK_SIZE = 1024 * 1024  # 1 MiB
MAX_CACHE_BYTES = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB cap
RELAY_IDLE_SEC = 45
START_TIME = time.time()
NATIVE_AUDIO_CODECS = {"aac", "mp3", "opus", "vorbis", "flac"}
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "mov_text", "webvtt", "text"}
DEVICE_ID_RE = re.compile(r"^[a-zA-Z0-9_-]{8,64}$")

LANG_NAMES = {
    "tam": "Tamil", "ta": "Tamil", "tamil": "Tamil",
    "hin": "Hindi", "hi": "Hindi", "hindi": "Hindi",
    "tel": "Telugu", "te": "Telugu", "telugu": "Telugu",
    "mal": "Malayalam", "ml": "Malayalam", "malayalam": "Malayalam",
    "kan": "Kannada", "kn": "Kannada", "kannada": "Kannada",
    "eng": "English", "en": "English", "english": "English",
    "jpn": "Japanese", "jap": "Japanese", "ja": "Japanese",
    "kor": "Korean", "ko": "Korean",
    "spa": "Spanish", "es": "Spanish",
    "fre": "French", "fra": "French", "fr": "French",
}

for d in (DATA_DIR, CACHE_DIR, HLS_DIR):
    d.mkdir(parents=True, exist_ok=True)

DEFAULT_CHANNELS = [
    {
        "id": "suntv",
        "name": "Sun TV",
        "group": "Tamil",
        "mode": "relay",
        "source_url": os.getenv("SUNTV_SOURCE_URL") or DEFAULT_SUNTV_URL,
    }
]

app = FastAPI(title="YStream Server")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

tg_client = Client(
    name=str(SESSION_FILE),
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir=str(DATA_DIR),
)

ffmpeg_proc: Optional[subprocess.Popen] = None
active_play_proc: Optional[asyncio.subprocess.Process] = None
last_error: str = ""
last_relay_hit: float = 0.0
msg_cache: dict[str, Message] = {}
prewarm_tasks: dict[str, asyncio.Task] = {}

# Per-device real-time SSE queues: device_id -> set[asyncio.Queue]
device_subscribers: dict[str, set[asyncio.Queue]] = {}
# Pending file shares awaiting device selection: share_id -> {"item": dict, "selected": set[str]}
pending_shares: dict[str, dict] = {}
last_device_disk_sync: dict[str, float] = {}


def read_json(path: Path, default):
    if not path.exists():
        path.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, data):
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


read_json(LIBRARY_FILE, [])
read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
DEVICES_STORE: dict[str, dict] = read_json(DEVICES_FILE, {})
if not isinstance(DEVICES_STORE, dict):
    DEVICES_STORE = {}
    write_json(DEVICES_FILE, DEVICES_STORE)

SYSTEM_STOPPED: bool = bool(read_json(STATE_FILE, {"stopped": False}).get("stopped", False))


def save_devices_store():
    write_json(DEVICES_FILE, DEVICES_STORE)


def push_to_device(device_id: str, payload: dict):
    subs = device_subscribers.get(device_id)
    if not subs:
        return
    raw = json.dumps(payload, ensure_ascii=False)
    for q in list(subs):
        try:
            q.put_nowait(raw)
        except Exception:
            pass


def push_to_all_devices(payload: dict):
    raw = json.dumps(payload, ensure_ascii=False)
    for subs in list(device_subscribers.values()):
        for q in list(subs):
            try:
                q.put_nowait(raw)
            except Exception:
                pass


def set_system_stopped(stopped: bool):
    global SYSTEM_STOPPED
    SYSTEM_STOPPED = bool(stopped)
    write_json(STATE_FILE, {"stopped": SYSTEM_STOPPED, "updated_at": int(time.time())})
    push_to_all_devices({"type": "system_state", "stopped": SYSTEM_STOPPED, "ts": int(time.time())})


def get_owner_id() -> int:
    if ENV_OWNER_ID > 0:
        return ENV_OWNER_ID
    data = read_json(OWNER_FILE, {"owner_id": 0})
    try:
        return int(data.get("owner_id") or 0)
    except Exception:
        return 0


def authorize_user(user: Optional[User], allow_claim: bool = False) -> bool:
    if not user or getattr(user, "is_bot", False):
        return False
    uid = int(user.id)
    owner = get_owner_id()
    if owner > 0:
        return uid == owner
    if allow_claim:
        write_json(OWNER_FILE, {"owner_id": uid, "claimed_at": int(time.time())})
        return True
    return False


def sanitize_device_id(raw: str) -> str:
    val = str(raw or "").strip()
    return val if DEVICE_ID_RE.match(val) else ""


def sanitize_device_name(raw: str) -> str:
    clean = re.sub(r"[^\w\s().-]", "", str(raw or "")).strip()
    return clean[:36] if clean else "Unknown Device"


def is_device_online(device_id: str) -> bool:
    if device_subscribers.get(device_id):
        return True
    dev = DEVICES_STORE.get(device_id)
    if not dev:
        return False
    return (time.time() - float(dev.get("last_active") or 0)) < 25.0


def fmt_relative_time(ts: float) -> str:
    if not ts or ts <= 0:
        return "Never"
    diff = int(max(0, time.time() - ts))
    if diff < 15:
        return "Just now"
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return f"{diff // 86400}d ago"


def extract_device_creds(request: Request, did: str = "", dtok: str = "") -> tuple[str, str]:
    raw_id = did or request.headers.get("x-device-id") or request.query_params.get("did") or ""
    raw_tok = dtok or request.headers.get("x-device-token") or request.query_params.get("dtok") or ""
    return sanitize_device_id(raw_id), str(raw_tok or "").strip()


def require_approved_device(request: Request, did: str = "", dtok: str = "", check_stopped: bool = True) -> dict:
    dev_id, dev_tok = extract_device_creds(request, did, dtok)
    if not dev_id or not dev_tok:
        raise HTTPException(status_code=401, detail="Device approval required")

    dev = DEVICES_STORE.get(dev_id)
    if not dev or dev.get("status") != "approved":
        raise HTTPException(status_code=403, detail="Device not approved")

    stored_tok = str(dev.get("device_token") or "")
    if not stored_tok or not secrets.compare_digest(stored_tok, dev_tok):
        raise HTTPException(status_code=403, detail="Invalid device credentials")

    now = time.time()
    dev["last_active"] = int(now)
    if now - last_device_disk_sync.get(dev_id, 0.0) > 20.0:
        last_device_disk_sync[dev_id] = now
        save_devices_store()

    if check_stopped and (SYSTEM_STOPPED or bool(dev.get("stopped", False))):
        raise HTTPException(status_code=503, detail="System shutdown")

    return dev


def get_active_tunnel_url() -> str:
    for cmd in (
        ["journalctl", "-u", "ystream-tunnel", "-n", "80", "--no-pager"],
        ["sudo", "-n", "journalctl", "-u", "ystream-tunnel", "-n", "80", "--no-pager"],
    ):
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            matches = re.findall(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", out)
            if matches:
                return matches[-1]
        except Exception:
            pass
    return ""


def get_sys_stats() -> dict:
    mem_used_mb, mem_total_mb = 0, 0
    try:
        info = {}
        for line in Path("/proc/meminfo").read_text().splitlines():
            k, v = line.split(":", 1)
            info[k.strip()] = int(v.strip().split()[0])
        total_kb = info.get("MemTotal", 0)
        avail_kb = info.get("MemAvailable", info.get("MemFree", 0))
        mem_total_mb = total_kb // 1024
        mem_used_mb = max(0, (total_kb - avail_kb) // 1024)
    except Exception:
        pass

    load_1m = 0.0
    try:
        load_1m = os.getloadavg()[0]
    except Exception:
        pass

    disk_used_gb, disk_total_gb = 0.0, 0.0
    try:
        du = shutil.disk_usage("/")
        disk_total_gb = du.total / (1024 ** 3)
        disk_used_gb = du.used / (1024 ** 3)
    except Exception:
        pass

    return {
        "mem_used_mb": mem_used_mb,
        "mem_total_mb": mem_total_mb,
        "load_1m": load_1m,
        "disk_used_gb": disk_used_gb,
        "disk_total_gb": disk_total_gb,
    }


def fmt_uptime(sec: float) -> str:
    s = int(max(0, sec))
    d, s = divmod(s, 86400)
    h, s = divmod(s, 3600)
    m, s = divmod(s, 60)
    if d > 0:
        return f"{d}d {h}h {m}m"
    if h > 0:
        return f"{h}h {m}m"
    return f"{m}m {s}s"


def detect_quality(title: str, width: int = 0, height: int = 0) -> str:
    if width >= 3800 or height >= 2100:
        return "4K"
    if width >= 2500 or height >= 1400:
        return "2K"
    if width >= 1900 or height >= 1040:
        return "1080p"
    if width >= 1260 or height >= 700:
        return "720p"
    if width >= 800 or height >= 460:
        return "480p"
    if height > 0:
        return f"{height}p"

    low = (title or "").lower()
    if re.search(r"\b(2160p|4k|uhd)\b", low):
        return "4K"
    if re.search(r"\b(1440p|2k|qhd)\b", low):
        return "2K"
    if re.search(r"\b(1080p|fhd)\b", low):
        return "1080p"
    if re.search(r"\b(720p|hd)\b", low):
        return "720p"
    if re.search(r"\b(480p|sd)\b", low):
        return "480p"
    if re.search(r"\b360p\b", low):
        return "360p"
    return "HD"


def fmt_bytes(size: int) -> str:
    if not size or size <= 0:
        return "0 MB"
    gb = size / (1024 * 1024 * 1024)
    if gb >= 1.0:
        return f"{gb:.2f} GB"
    return f"{size / (1024 * 1024):.1f} MB"


def resolve_lang_label(lang_raw: str, title_raw: str, fallback: str) -> tuple[str, bool]:
    l_clean = (lang_raw or "").strip().lower()
    t_clean = (title_raw or "").strip()
    t_low = t_clean.lower()

    if l_clean in LANG_NAMES:
        name = LANG_NAMES[l_clean]
        return name, name == "Tamil"

    for key, val in LANG_NAMES.items():
        if len(key) >= 3 and re.search(rf"\b{re.escape(key)}\b", t_low):
            return val, val == "Tamil"

    if t_clean and t_clean.lower() not in ("und", "unknown", "stereo", "aac", "ac3", "eac3"):
        return t_clean[:18], "tamil" in t_low
    if l_clean and l_clean != "und":
        return l_clean.upper()[:8], False
    return fallback, False


def is_video_msg(msg: Message) -> bool:
    if msg.video or msg.animation:
        return True
    if msg.document:
        mime = (msg.document.mime_type or "").lower()
        name = (msg.document.file_name or "").lower()
        if mime.startswith("video/") or name.endswith((".mp4", ".mkv", ".webm", ".mov", ".avi", ".m4v", ".ts")):
            return True
    return False


def get_media_info(msg: Message) -> Optional[dict]:
    if not is_video_msg(msg):
        return None
    media = msg.video or msg.document or msg.animation
    name = getattr(media, "file_name", None) or msg.caption or f"Movie_{msg.id}.mp4"
    title = str(name).strip()
    mime = getattr(media, "mime_type", None) or "video/mp4"
    size = getattr(media, "file_size", None) or 0
    duration = getattr(media, "duration", None) or 0
    width = int(getattr(media, "width", 0) or 0)
    height = int(getattr(media, "height", 0) or 0)
    low = title.lower()
    likely_compat = bool(re.search(r"\b(ac3|dd5\.1|ddp|eac3|dts|truehd|atmos)\b", low))
    return {
        "id": f"{msg.chat.id}_{msg.id}",
        "chat_id": msg.chat.id,
        "message_id": msg.id,
        "title": title,
        "mime_type": mime,
        "file_size": int(size),
        "duration": int(duration),
        "width": width,
        "height": height,
        "quality": detect_quality(title, width, height),
        "audio_codec": "",
        "compat_audio": likely_compat,
        "audio_tracks": [],
        "default_audio_idx": 0,
        "subtitles": [],
        "allowed_devices": [],
        "probed": False,
        "added_at": int(time.time()),
    }


def safe_item_dir(item_id: str) -> Path:
    safe = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
    p = CACHE_DIR / safe
    p.mkdir(parents=True, exist_ok=True)
    return p


def block_path(item_id: str, b_idx: int) -> Path:
    return safe_item_dir(item_id) / f"{b_idx}.blk"


def write_block_sync(path: Path, data: bytes):
    if path.exists():
        return
    tmp = path.with_suffix(f".{os.getpid()}.{time.monotonic_ns()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def get_cache_stats_sync() -> tuple[int, int]:
    files = [f for f in CACHE_DIR.glob("*/*.blk") if f.exists()]
    return len(files), sum(f.stat().st_size for f in files)


def clear_all_cache_sync():
    for child in CACHE_DIR.iterdir():
        try:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink()
        except Exception:
            pass


def prune_cache_sync():
    files = list(CACHE_DIR.glob("*/*.blk"))
    total = sum(f.stat().st_size for f in files if f.exists())
    if total <= MAX_CACHE_BYTES:
        return
    files.sort(key=lambda f: f.stat().st_mtime)
    for f in files:
        if total <= int(MAX_CACHE_BYTES * 0.7):
            break
        try:
            sz = f.stat().st_size
            f.unlink()
            total -= sz
        except Exception:
            pass


def run_ffprobe_json(target: str) -> Optional[dict]:
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-analyzeduration", "2500000",
            "-probesize", "2500000",
            "-show_entries", "format=duration:stream=codec_type,codec_name,width,height:stream_tags=language,title",
            "-of", "json", target,
        ]
        res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL, timeout=7)
        return json.loads(res)
    except Exception:
        return None


def parse_ffprobe_data(data: dict) -> Optional[dict]:
    streams = data.get("streams") or []
    if not streams:
        return None
    v_stream = next((s for s in streams if s.get("codec_type") == "video"), {})
    fmt_info = data.get("format") or {}

    audio_tracks = []
    a_idx = 0
    for s in streams:
        if s.get("codec_type") != "audio":
            continue
        tags = s.get("tags") or {}
        codec = str(s.get("codec_name") or "").lower()
        label, is_tamil = resolve_lang_label(tags.get("language", ""), tags.get("title", ""), f"Audio {a_idx + 1}")
        audio_tracks.append({
            "index": a_idx,
            "label": label,
            "codec": codec,
            "is_tamil": is_tamil,
        })
        a_idx += 1

    default_audio_idx = 0
    for tr in audio_tracks:
        if tr["is_tamil"]:
            default_audio_idx = tr["index"]
            break

    subtitles = []
    s_idx = 0
    for s in streams:
        if s.get("codec_type") != "subtitle":
            continue
        codec = str(s.get("codec_name") or "").lower()
        if codec in TEXT_SUB_CODECS:
            tags = s.get("tags") or {}
            label, _ = resolve_lang_label(tags.get("language", ""), tags.get("title", ""), f"Sub {len(subtitles) + 1}")
            subtitles.append({
                "index": s_idx,
                "label": label,
                "codec": codec,
            })
        s_idx += 1

    first_codec = audio_tracks[0]["codec"] if audio_tracks else ""
    compat = bool(first_codec and first_codec not in NATIVE_AUDIO_CODECS)
    w = int(v_stream.get("width") or 0)
    h = int(v_stream.get("height") or 0)
    dur = 0
    try:
        dur = int(float(fmt_info.get("duration") or 0))
    except Exception:
        pass

    return {
        "audio_codec": first_codec,
        "compat_audio": compat,
        "audio_tracks": audio_tracks,
        "default_audio_idx": default_audio_idx,
        "subtitles": subtitles,
        "width": w,
        "height": h,
        "duration": dur,
        "probed": True,
    }


def probe_cached_media_sync(item_id: str) -> Optional[dict]:
    b0 = block_path(item_id, 0)
    if b0.exists():
        b1 = block_path(item_id, 1)
        tmp_probe = safe_item_dir(item_id) / "probe.tmp"
        try:
            with tmp_probe.open("wb") as out:
                out.write(b0.read_bytes())
                if b1.exists():
                    out.write(b1.read_bytes())
            raw = run_ffprobe_json(str(tmp_probe))
            if raw:
                parsed = parse_ffprobe_data(raw)
                if parsed and (parsed["width"] > 0 or parsed["audio_tracks"]):
                    return parsed
        finally:
            try:
                tmp_probe.unlink()
            except Exception:
                pass
    return None


def update_library_probe(item_id: str, info: dict):
    items = read_json(LIBRARY_FILE, [])
    changed = False
    for row in items:
        if row.get("id") == item_id:
            row["probed"] = True
            row["audio_tracks"] = info.get("audio_tracks", [])
            row["default_audio_idx"] = int(info.get("default_audio_idx", 0))
            row["subtitles"] = info.get("subtitles", [])
            if info.get("audio_codec"):
                row["audio_codec"] = info["audio_codec"]
                row["compat_audio"] = bool(info["compat_audio"])
            if info.get("width") and not row.get("width"):
                row["width"] = info["width"]
            if info.get("height") and not row.get("height"):
                row["height"] = info["height"]
            if info.get("duration") and not row.get("duration"):
                row["duration"] = info["duration"]
            row["quality"] = detect_quality(
                row.get("title", ""),
                int(row.get("width") or 0),
                int(row.get("height") or 0),
            )
            changed = True
            break
    if changed:
        write_json(LIBRARY_FILE, items)


async def kill_active_play_proc():
    global active_play_proc
    proc = active_play_proc
    active_play_proc = None
    if proc and proc.returncode is None:
        try:
            proc.kill()
            await proc.wait()
        except Exception:
            pass


async def clear_queue_and_cache():
    await kill_active_play_proc()
    for task in list(prewarm_tasks.values()):
        task.cancel()
    prewarm_tasks.clear()
    msg_cache.clear()
    write_json(LIBRARY_FILE, [])
    await asyncio.to_thread(clear_all_cache_sync)
    push_to_all_devices({"type": "library_updated"})


async def get_tg_message(item: dict, force_refresh: bool = False) -> Message:
    item_id = item["id"]
    if not force_refresh and item_id in msg_cache:
        return msg_cache[item_id]
    if len(msg_cache) > 40:
        msg_cache.clear()
    msg = await tg_client.get_messages(int(item["chat_id"]), int(item["message_id"]))
    media = msg.video or msg.document or msg.animation
    if not media:
        raise HTTPException(status_code=404, detail="Media missing in Telegram")
    msg_cache[item_id] = msg
    return msg


async def cache_span(item: dict, start_block: int, count: int):
    if count <= 0:
        return
    msg = await get_tg_message(item)
    buf = bytearray()
    curr_b = start_block
    end_b = start_block + count - 1
    async for chunk in tg_client.stream_media(msg, offset=start_block):
        buf.extend(chunk)
        while len(buf) >= CHUNK_SIZE and curr_b <= end_b:
            blk = bytes(buf[:CHUNK_SIZE])
            del buf[:CHUNK_SIZE]
            await asyncio.to_thread(write_block_sync, block_path(item["id"], curr_b), blk)
            curr_b += 1
        if curr_b > end_b:
            break
    if buf and curr_b <= end_b:
        await asyncio.to_thread(write_block_sync, block_path(item["id"], curr_b), bytes(buf))


async def prewarm_head_tail(item: dict):
    item_id = item["id"]
    try:
        msg = await get_tg_message(item)
        media = msg.video or msg.document or msg.animation
        total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
        if total_size <= 0:
            return
        total_blocks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE

        head_count = min(2, total_blocks)
        if not all(block_path(item_id, b).exists() for b in range(head_count)):
            await cache_span(item, 0, head_count)

        if total_blocks > 3 and not block_path(item_id, total_blocks - 1).exists():
            await cache_span(item, total_blocks - 1, 1)

        probe_res = await asyncio.to_thread(probe_cached_media_sync, item_id)
        if probe_res:
            update_library_probe(item_id, probe_res)

        await asyncio.to_thread(prune_cache_sync)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[Prewarm {item_id}] {exc}")
    finally:
        prewarm_tasks.pop(item_id, None)


def trigger_prewarm(item: dict):
    item_id = item["id"]
    if item_id in prewarm_tasks:
        return
    if block_path(item_id, 0).exists() and item.get("probed"):
        return
    prewarm_tasks[item_id] = asyncio.create_task(prewarm_head_tail(item))


def save_media_for_devices(item: dict, target_devices: list[str]) -> tuple[bool, int]:
    items = read_json(LIBRARY_FILE, [])
    for row in items:
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            existing = set(row.get("allowed_devices") or [])
            existing.update(target_devices)
            row["allowed_devices"] = list(existing)
            write_json(LIBRARY_FILE, items)
            trigger_prewarm(row)
            return False, len(items)
    item["allowed_devices"] = list(dict.fromkeys(target_devices))
    items.append(item)
    write_json(LIBRARY_FILE, items)
    trigger_prewarm(item)
    return True, len(items)


# --- Telegram UI Builders (Approval, /device Remote, File Share Selector) ---
async def send_device_approval_alert(dev: dict, is_reaccess: bool = False):
    owner_id = get_owner_id()
    if owner_id <= 0:
        return
    did = dev["device_id"]
    dname = html.escape(dev.get("device_name") or "Unknown Device")
    header = "🔄 <b>Re-Access Request</b>" if is_reaccess else "🔐 <b>New Device Approval Required</b>"
    text = (
        f"{header}\n\n"
        f"<b>Device:</b> {dname}\n"
        f"<b>Device ID:</b> <code>{html.escape(did)}</code>\n"
        f"<b>Status:</b> Pending Approval\n\n"
        f"Tap <b>Approve</b> to grant permanent access or <b>Cancel</b> to block."
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Approve", callback_data=f"dev_app:{did}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"dev_rej:{did}"),
        ]
    ])
    try:
        sent = await tg_client.send_message(owner_id, text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
        dev["notify_msg_id"] = sent.id
        dev["last_notified_at"] = int(time.time())
        save_devices_store()
    except Exception as exc:
        print(f"[Approval notify error] {exc}")


def build_devices_list_view() -> tuple[str, InlineKeyboardMarkup]:
    approved = [d for d in DEVICES_STORE.values() if d.get("status") == "approved"]
    pending = [d for d in DEVICES_STORE.values() if d.get("status") == "pending"]
    approved.sort(key=lambda d: (0 if is_device_online(d["device_id"]) else 1, -(d.get("last_active") or 0)))

    if not approved and not pending:
        txt = (
            "<b>📱 Registered Devices</b>\n\n"
            "No devices registered yet.\n"
            "Open YStream on your TV or browser to request approval."
        )
        return txt, InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="dev_list")]])

    lines = ["<b>📱 Device Management</b>", "Select a device to open its Remote Control:\n"]
    rows = []

    for d in approved:
        did = d["device_id"]
        online = is_device_online(did)
        dot = "🟢" if online else "⚪"
        state_lbl = "Online" if online else f"Offline ({fmt_relative_time(d.get('last_active', 0))})"
        power_tag = " • 🛑 Shutdown" if d.get("stopped") else ""
        lines.append(f"{dot} <b>{html.escape(d.get('device_name', 'Device'))}</b> — {state_lbl}{power_tag}")
        rows.append([
            InlineKeyboardButton(
                f"{dot} {d.get('device_name', 'Device')[:26]}",
                callback_data=f"dev_sel:{did}",
            )
        ])

    for d in pending:
        did = d["device_id"]
        lines.append(f"🟡 <b>{html.escape(d.get('device_name', 'Device'))}</b> — Pending Approval")
        rows.append([
            InlineKeyboardButton(f"✅ Approve {d.get('device_name', 'Device')[:18]}", callback_data=f"dev_app:{did}"),
            InlineKeyboardButton("❌ Cancel", callback_data=f"dev_rej:{did}"),
        ])

    rows.append([InlineKeyboardButton("🔄 Refresh", callback_data="dev_list")])
    return "\n".join(lines), InlineKeyboardMarkup(rows)


def build_device_remote_view(did: str) -> tuple[str, InlineKeyboardMarkup]:
    dev = DEVICES_STORE.get(did)
    if not dev or dev.get("status") != "approved":
        return build_devices_list_view()

    online = is_device_online(did)
    dot = "🟢 Online" if online else "⚪ Offline"
    stopped = bool(dev.get("stopped", False))
    power_status = "🛑 Shutdown (Screen Locked)" if stopped else "▶️ Active"
    last_seen = fmt_relative_time(dev.get("last_active", 0))

    text = (
        f"🎮 <b>Remote: {html.escape(dev.get('device_name', 'Device'))}</b>\n"
        f"<b>Status:</b> {dot}  |  <b>Last Active:</b> {last_seen}\n"
        f"<b>Power:</b> {power_status}\n"
        f"<b>ID:</b> <code>{html.escape(did)}</code>"
    )

    power_btn = (
        InlineKeyboardButton("▶️ Resume Device", callback_data=f"rc:{did}:resume")
        if stopped
        else InlineKeyboardButton("🛑 Shutdown Device", callback_data=f"rc:{did}:shutdown")
    )

    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📺 Live TV", callback_data=f"rc:{did}:live"),
            InlineKeyboardButton("🎬 YPlay", callback_data=f"rc:{did}:yplay"),
            InlineKeyboardButton("☰ Menu", callback_data=f"rc:{did}:menu"),
        ],
        [
            InlineKeyboardButton("⬆️ Up / Ch+", callback_data=f"rc:{did}:up"),
        ],
        [
            InlineKeyboardButton("⬅️ Left", callback_data=f"rc:{did}:left"),
            InlineKeyboardButton("🆗 OK", callback_data=f"rc:{did}:ok"),
            InlineKeyboardButton("➡️ Right", callback_data=f"rc:{did}:right"),
        ],
        [
            InlineKeyboardButton("🔙 Back", callback_data=f"rc:{did}:back"),
            InlineKeyboardButton("⬇️ Down / Ch-", callback_data=f"rc:{did}:down"),
        ],
        [
            InlineKeyboardButton("⏪ -10s", callback_data=f"rc:{did}:seek_back"),
            InlineKeyboardButton("⏯ Play/Pause", callback_data=f"rc:{did}:playpause"),
            InlineKeyboardButton("⏩ +10s", callback_data=f"rc:{did}:seek_fwd"),
        ],
        [
            InlineKeyboardButton("🔇 Mute / Unmute", callback_data=f"rc:{did}:mute"),
            InlineKeyboardButton("⏭ Next", callback_data=f"rc:{did}:next"),
        ],
        [power_btn],
        [
            InlineKeyboardButton("🗑 Remove Device", callback_data=f"dev_rm_ask:{did}"),
            InlineKeyboardButton("⬅️ Devices", callback_data="dev_list"),
        ],
    ])
    return text, kb


def build_share_selector_view(share_id: str) -> tuple[str, InlineKeyboardMarkup]:
    entry = pending_shares.get(share_id)
    if not entry:
        return "<b>Session expired.</b> Forward the file again.", InlineKeyboardMarkup([])
    item = entry["item"]
    selected: set[str] = entry["selected"]
    approved = [d for d in DEVICES_STORE.values() if d.get("status") == "approved"]

    text = (
        f"🎬 <b>Select Device(s) to Share</b>\n\n"
        f"<b>File:</b> {html.escape(item['title'])}\n"
        f"<b>Quality:</b> <code>{html.escape(item['quality'])}</code> • <code>{fmt_bytes(item['file_size'])}</code>\n\n"
        f"Tap device(s) below, then press <b>Confirm & Share</b>:"
    )

    rows = []
    for d in approved:
        did = d["device_id"]
        checked = "✅" if did in selected else "⬜"
        online = "🟢" if is_device_online(did) else "⚪"
        rows.append([
            InlineKeyboardButton(
                f"{checked} {d.get('device_name', 'Device')[:24]} ({online})",
                callback_data=f"sh_tog:{share_id}:{did}",
            )
        ])

    rows.append([
        InlineKeyboardButton(f"🚀 Confirm & Share ({len(selected)})", callback_data=f"sh_go:{share_id}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"sh_no:{share_id}"),
    ])
    return text, InlineKeyboardMarkup(rows)


async def build_status_html() -> str:
    items = read_json(LIBRARY_FILE, [])
    blk_cnt, blk_bytes = await asyncio.to_thread(get_cache_stats_sync)
    sys = await asyncio.to_thread(get_sys_stats)
    tunnel = get_active_tunnel_url()
    owner_id = get_owner_id()
    approved_cnt = sum(1 for d in DEVICES_STORE.values() if d.get("status") == "approved")
    online_cnt = sum(1 for d in DEVICES_STORE.values() if d.get("status") == "approved" and is_device_online(d["device_id"]))

    rows = [
        ("Bot", "Connected" if tg_client.is_connected else "Offline"),
        ("System", "SHUTDOWN" if SYSTEM_STOPPED else "ACTIVE"),
        ("Devices", f"{online_cnt}/{approved_cnt} online"),
        ("Owner Lock", str(owner_id) if owner_id else "Unlocked"),
        ("Queue", f"{len(items)} movie(s)"),
        ("Cache", f"{fmt_bytes(blk_bytes)} ({blk_cnt} blk)"),
        ("RAM", f"{sys['mem_used_mb']}/{sys['mem_total_mb']} MB"),
        ("Disk", f"{sys['disk_used_gb']:.1f}/{sys['disk_total_gb']:.1f} GB"),
        ("CPU Load", f"{sys['load_1m']:.2f}"),
        ("Tunnel", "Online" if tunnel else "Missing"),
        ("Uptime", fmt_uptime(time.time() - START_TIME)),
    ]

    w1, w2 = 10, 19
    sep = f"+-{'-'*w1}-+-{'-'*w2}-+"
    lines = [sep, f"| {'Component':<{w1}} | {'Diagnosis':<{w2}} |", sep]
    for k, v in rows:
        lines.append(f"| {k:<{w1}} | {v[:w2]:<{w2}} |")
    lines.append(sep)
    table = "\n".join(lines)

    out = f"<b>YStream Full Diagnosis</b>\n<pre>{html.escape(table)}</pre>"
    if tunnel:
        out += f"\n\n<b>Tunnel URL:</b>\n<code>{html.escape(tunnel)}</code>"
    return out


# --- Owner-Only Telegram Bot Handlers ---
@tg_client.on_message(filters.private & filters.command("start"))
async def cmd_start(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    state_txt = "SYSTEM SHUTDOWN (send /resume to restore)" if SYSTEM_STOPPED else "ACTIVE"
    await msg.reply_text(
        f"<b>YStream Bot Ready.</b>\n"
        f"Status: <code>{state_txt}</code>\n"
        f"Forward any movie to choose target device(s).\n"
        f"Commands: /device, /status, /shutdown, /resume, /clear",
        parse_mode=enums.ParseMode.HTML,
    )


@tg_client.on_message(filters.private & filters.command(["device", "devices"]))
async def cmd_device(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    txt, kb = build_devices_list_view()
    await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


@tg_client.on_message(filters.private & filters.command("shutdown"))
async def cmd_shutdown(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    set_system_stopped(True)
    await kill_active_play_proc()
    stop_suntv_relay()
    await msg.reply_text(
        "<b>Global System Shutdown Activated.</b>\nAll devices are locked with the System Shutdown screen.\nSend /resume to restore.",
        parse_mode=enums.ParseMode.HTML,
    )


@tg_client.on_message(filters.private & filters.command(["resume", "poweron"]))
async def cmd_resume(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    set_system_stopped(False)
    for d in DEVICES_STORE.values():
        d["stopped"] = False
    save_devices_store()
    push_to_all_devices({"type": "device_state", "stopped": False})
    await msg.reply_text(
        "<b>System Resumed.</b>\nPlayback restored immediately.",
        parse_mode=enums.ParseMode.HTML,
    )


@tg_client.on_message(filters.private & filters.command("status"))
async def cmd_status(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    txt = await build_status_html()
    await msg.reply_text(
        txt,
        parse_mode=enums.ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="cb:status")]]),
    )


@tg_client.on_message(filters.private & filters.command("clear"))
async def cmd_clear(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    await clear_queue_and_cache()
    await msg.reply_text(
        "<b>Cleared All Movies & Cache.</b>\nNext video sent starts at <code>#1</code>.",
        parse_mode=enums.ParseMode.HTML,
    )


@tg_client.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def on_file(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    info = get_media_info(msg)
    if not info:
        return

    approved = [d for d in DEVICES_STORE.values() if d.get("status") == "approved"]
    if not approved:
        await msg.reply_text(
            "⚠️ <b>No Approved Devices Found.</b>\nOpen YStream on your TV or browser and approve the device first.",
            parse_mode=enums.ParseMode.HTML,
        )
        return

    share_id = secrets.token_hex(6)
    default_sel = {approved[0]["device_id"]} if len(approved) == 1 else set()
    pending_shares[share_id] = {"item": info, "selected": default_sel}
    if len(pending_shares) > 30:
        oldest = next(iter(pending_shares))
        pending_shares.pop(oldest, None)

    txt, kb = build_share_selector_view(share_id)
    await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


@tg_client.on_callback_query()
async def on_callback(_, cq: CallbackQuery):
    if not authorize_user(cq.from_user, allow_claim=False):
        return
    data = cq.data or ""

    try:
        if data == "cb:status":
            await cq.answer("Refreshing...")
            txt = await build_status_html()
            await cq.message.edit_text(
                txt,
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="cb:status")]]),
            )
            return

        if data == "dev_list":
            await cq.answer()
            txt, kb = build_devices_list_view()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev_app:"):
            did = sanitize_device_id(data.split(":", 1)[1])
            dev = DEVICES_STORE.get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            token = secrets.token_urlsafe(24)
            dev["status"] = "approved"
            dev["device_token"] = token
            dev["token_claimed"] = False
            dev["stopped"] = False
            dev["approved_at"] = int(time.time())
            save_devices_store()

            push_to_device(did, {
                "type": "auth_update",
                "status": "approved",
                "device_token": token,
                "stopped": SYSTEM_STOPPED,
            })
            await cq.answer("✅ Device Approved!")
            dname = html.escape(dev.get("device_name", "Device"))
            await cq.message.edit_text(
                f"✅ <b>Device Approved</b>\n\n"
                f"<b>Device:</b> {dname}\n"
                f"<b>ID:</b> <code>{html.escape(did)}</code>\n"
                f"Permanent access granted.",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🎮 Open Remote Control", callback_data=f"dev_sel:{did}")],
                    [InlineKeyboardButton("📱 All Devices", callback_data="dev_list")],
                ]),
            )
            return

        if data.startswith("dev_rej:"):
            did = sanitize_device_id(data.split(":", 1)[1])
            dev = DEVICES_STORE.get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            dev["status"] = "rejected"
            dev["device_token"] = ""
            save_devices_store()
            push_to_device(did, {"type": "auth_update", "status": "rejected"})
            await cq.answer("❌ Request Cancelled")
            await cq.message.edit_text(
                f"❌ <b>Access Request Cancelled</b>\n\n"
                f"<b>Device:</b> {html.escape(dev.get('device_name', 'Device'))}\n"
                f"<b>ID:</b> <code>{html.escape(did)}</code>\n"
                f"Access remains blocked.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data.startswith("dev_sel:"):
            did = sanitize_device_id(data.split(":", 1)[1])
            await cq.answer()
            txt, kb = build_device_remote_view(did)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev_rm_ask:"):
            did = sanitize_device_id(data.split(":", 1)[1])
            dev = DEVICES_STORE.get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            await cq.answer()
            await cq.message.edit_text(
                f"⚠️ <b>Remove Device?</b>\n\n"
                f"<b>Device:</b> {html.escape(dev.get('device_name', 'Device'))}\n"
                f"<b>ID:</b> <code>{html.escape(did)}</code>\n\n"
                f"This will immediately revoke its access and stop playback.",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton("🗑 Yes, Remove", callback_data=f"dev_rm_yes:{did}"),
                        InlineKeyboardButton("↩️ Cancel", callback_data=f"dev_sel:{did}"),
                    ]
                ]),
            )
            return

        if data.startswith("dev_rm_yes:"):
            did = sanitize_device_id(data.split(":", 1)[1])
            dev = DEVICES_STORE.get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            dev["status"] = "revoked"
            dev["device_token"] = ""
            dev["token_claimed"] = False
            dev["stopped"] = False
            save_devices_store()

            # Remove device from allowed_devices on all library items
            items = read_json(LIBRARY_FILE, [])
            changed = False
            for row in items:
                allowed = row.get("allowed_devices") or []
                if did in allowed:
                    row["allowed_devices"] = [x for x in allowed if x != did]
                    changed = True
            if changed:
                write_json(LIBRARY_FILE, items)

            push_to_device(did, {"type": "auth_update", "status": "revoked"})
            await cq.answer("🗑 Device Removed & Access Revoked")
            txt, kb = build_devices_list_view()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("rc:"):
            parts = data.split(":", 2)
            if len(parts) != 3:
                return
            did = sanitize_device_id(parts[1])
            action = parts[2]
            dev = DEVICES_STORE.get(did)
            if not dev or dev.get("status") != "approved":
                await cq.answer("Device not approved", show_alert=True)
                return

            if action == "shutdown":
                dev["stopped"] = True
                save_devices_store()
                await kill_active_play_proc()
                push_to_device(did, {"type": "device_state", "stopped": True, "ts": int(time.time())})
                await cq.answer("🛑 Device Shutdown Activated")
                txt, kb = build_device_remote_view(did)
                await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
                return

            if action == "resume":
                dev["stopped"] = False
                save_devices_store()
                push_to_device(did, {"type": "device_state", "stopped": False, "ts": int(time.time())})
                await cq.answer("▶️ Device Resumed")
                txt, kb = build_device_remote_view(did)
                await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
                return

            if not is_device_online(did):
                await cq.answer("⚠️ Device is currently offline", show_alert=False)
            else:
                await cq.answer(f"Sent: {action.upper()}")

            push_to_device(did, {"type": "remote_cmd", "cmd": action, "ts": int(time.time() * 1000)})
            return

        if data.startswith("sh_tog:"):
            _, share_id, did = data.split(":", 2)
            entry = pending_shares.get(share_id)
            if not entry:
                await cq.answer("Share expired", show_alert=True)
                return
            did = sanitize_device_id(did)
            if did in entry["selected"]:
                entry["selected"].remove(did)
            else:
                entry["selected"].add(did)
            await cq.answer()
            txt, kb = build_share_selector_view(share_id)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("sh_go:"):
            share_id = data.split(":", 1)[1]
            entry = pending_shares.get(share_id)
            if not entry:
                await cq.answer("Share expired", show_alert=True)
                return
            selected_ids = [
                d for d in entry["selected"]
                if DEVICES_STORE.get(d, {}).get("status") == "approved"
            ]
            if not selected_ids:
                await cq.answer("Select at least 1 approved device!", show_alert=True)
                return

            item = entry["item"]
            pending_shares.pop(share_id, None)
            added, _ = save_media_for_devices(item, selected_ids)
            for target_did in selected_ids:
                push_to_device(target_did, {"type": "library_updated"})

            names = ", ".join(html.escape(DEVICES_STORE[d].get("device_name", d)) for d in selected_ids)
            await cq.answer("🚀 Shared!")
            await cq.message.edit_text(
                f"✅ <b>Shared Exclusively To:</b> {names}\n\n"
                f"<b>File:</b> {html.escape(item['title'])}\n"
                f"<b>Quality:</b> <code>{html.escape(item['quality'])}</code> • <code>{fmt_bytes(item['file_size'])}</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data.startswith("sh_no:"):
            share_id = data.split(":", 1)[1]
            pending_shares.pop(share_id, None)
            await cq.answer("Cancelled")
            await cq.message.edit_text("❌ <b>File share cancelled.</b>", parse_mode=enums.ParseMode.HTML)
            return

    except Exception as exc:
        print(f"[Callback error] {exc}")


# --- On-Demand Lightweight Relay ---
def stop_suntv_relay():
    global ffmpeg_proc
    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        ffmpeg_proc.terminate()
        try:
            ffmpeg_proc.wait(timeout=2)
        except Exception:
            ffmpeg_proc.kill()
    ffmpeg_proc = None
    for f in HLS_DIR.glob("*"):
        try:
            f.unlink()
        except Exception:
            pass


def format_ffmpeg_err(raw: str) -> str:
    low = (raw or "").lower()
    if "403" in low:
        return "CDN 403 (IP blocked)"
    if "404" in low:
        return "Stream 404"
    if "timed out" in low or "timeout" in low:
        return "Source timeout"
    return "Source offline"


async def start_suntv_relay():
    global ffmpeg_proc, last_error, last_relay_hit
    if SYSTEM_STOPPED:
        return False
    last_relay_hit = time.time()

    channels = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
    suntv = next((c for c in channels if c.get("id") == "suntv"), None)
    url = (suntv or {}).get("source_url") or os.getenv("SUNTV_SOURCE_URL") or DEFAULT_SUNTV_URL

    m3u8 = HLS_DIR / "index.m3u8"
    if ffmpeg_proc and ffmpeg_proc.poll() is None and m3u8.exists() and (time.time() - m3u8.stat().st_mtime) < 10:
        last_error = ""
        return True

    stop_suntv_relay()
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error",
        "-user_agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "-headers", "Accept: */*\r\nConnection: keep-alive\r\n",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-i", url,
        "-vf", "fps=25,scale=-2:720,format=yuv420p",
        "-c:v", "libx264", "-preset", "superfast", "-tune", "zerolatency",
        "-crf", "24", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0",
        "-c:a", "aac", "-b:a", "96k", "-ac", "2",
        "-f", "hls", "-hls_time", "2", "-hls_list_size", "4",
        "-hls_flags", "delete_segments+append_list+independent_segments",
        str(m3u8),
    ]
    try:
        ffmpeg_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except Exception:
        last_error = "FFmpeg error"
        return False

    for _ in range(24):
        if m3u8.exists() and len(list(HLS_DIR.glob("*.ts"))) >= 2:
            last_error = ""
            return True
        if ffmpeg_proc.poll() is not None:
            err = ffmpeg_proc.stderr.read().strip() if ffmpeg_proc.stderr else ""
            last_error = format_ffmpeg_err(err)
            return False
        await asyncio.sleep(0.25)

    last_error = "Source timeout"
    return False


async def relay_idle_watcher():
    while True:
        await asyncio.sleep(10)
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
            if (time.time() - last_relay_hit) > RELAY_IDLE_SEC:
                stop_suntv_relay()


@app.on_event("startup")
async def startup():
    await tg_client.start()
    try:
        await tg_client.set_bot_commands([
            BotCommand("device", "Manage Devices & Remote Control"),
            BotCommand("status", "Full Diagnosis Table"),
            BotCommand("shutdown", "Global System Shutdown"),
            BotCommand("resume", "Resume Playback"),
            BotCommand("clear", "Clear Queue & Cache"),
        ])
    except Exception as exc:
        print(f"[Bot commands error] {exc}")

    items = read_json(LIBRARY_FILE, [])
    if items:
        trigger_prewarm(items[0])
    asyncio.create_task(relay_idle_watcher())


@app.on_event("shutdown")
async def shutdown():
    await kill_active_play_proc()
    for t in list(prewarm_tasks.values()):
        t.cancel()
    stop_suntv_relay()
    try:
        await tg_client.stop()
    except Exception:
        pass


# --- Device Registration, Verification & Real-Time SSE Endpoints ---
@app.post("/api/device/register")
async def device_register(request: Request):
    try:
        body = await request.json()
    except Exception:
        body = {}

    did = sanitize_device_id(body.get("device_id", ""))
    if not did:
        raise HTTPException(status_code=400, detail="Invalid device_id")

    dname = sanitize_device_name(body.get("device_name", ""))
    dtok = str(body.get("device_token") or "").strip()
    force_request = bool(body.get("force_request", False))
    now = int(time.time())

    dev = DEVICES_STORE.get(did)
    if dev:
        if dname and dname != "Unknown Device":
            dev["device_name"] = dname
        dev["last_active"] = now
        status = dev.get("status", "pending")

        if status == "approved":
            stored_tok = str(dev.get("device_token") or "")
            if stored_tok and dtok and secrets.compare_digest(stored_tok, dtok):
                save_devices_store()
                return {
                    "status": "approved",
                    "device_id": did,
                    "device_token": stored_tok,
                    "stopped": SYSTEM_STOPPED or bool(dev.get("stopped", False)),
                }
            # Token missing/mismatched: require re-approval to prevent spoofing
            dev["status"] = "pending"
            dev["device_token"] = ""
            dev["token_claimed"] = False
            save_devices_store()
            await send_device_approval_alert(dev, is_reaccess=True)
            return {"status": "pending", "device_id": did}

        if status in ("revoked", "rejected"):
            if force_request:
                dev["status"] = "pending"
                dev["device_token"] = ""
                dev["token_claimed"] = False
                save_devices_store()
                await send_device_approval_alert(dev, is_reaccess=True)
                return {"status": "pending", "device_id": did}
            save_devices_store()
            return {"status": status, "device_id": did}

        # Currently pending
        last_notified = int(dev.get("last_notified_at") or 0)
        if force_request or (now - last_notified) > 30:
            await send_device_approval_alert(dev, is_reaccess=force_request)
        else:
            save_devices_store()
        return {"status": "pending", "device_id": did}

    # Brand new device
    new_dev = {
        "device_id": did,
        "device_token": "",
        "token_claimed": False,
        "device_name": dname,
        "status": "pending",
        "stopped": False,
        "created_at": now,
        "last_active": now,
        "last_notified_at": 0,
    }
    DEVICES_STORE[did] = new_dev
    save_devices_store()
    await send_device_approval_alert(new_dev, is_reaccess=False)
    return {"status": "pending", "device_id": did}


@app.get("/api/device/status")
async def device_status(request: Request, did: str = "", dtok: str = ""):
    dev_id, dev_tok = extract_device_creds(request, did, dtok)
    if not dev_id:
        raise HTTPException(status_code=400, detail="Missing device_id")
    dev = DEVICES_STORE.get(dev_id)
    if not dev:
        return {"status": "unregistered", "device_id": dev_id}

    dev["last_active"] = int(time.time())
    status = dev.get("status", "pending")
    if status == "approved":
        stored_tok = str(dev.get("device_token") or "")
        if stored_tok and dev_tok and secrets.compare_digest(stored_tok, dev_tok):
            return {
                "status": "approved",
                "device_id": dev_id,
                "stopped": SYSTEM_STOPPED or bool(dev.get("stopped", False)),
            }
        # One-time token handoff right after Telegram approval
        if stored_tok and not dev.get("token_claimed", False):
            dev["token_claimed"] = True
            save_devices_store()
            return {
                "status": "approved",
                "device_id": dev_id,
                "device_token": stored_tok,
                "stopped": SYSTEM_STOPPED or bool(dev.get("stopped", False)),
            }
        return {"status": "revoked", "device_id": dev_id}

    return {"status": status, "device_id": dev_id}


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "stopped": SYSTEM_STOPPED,
        "bot_connected": tg_client.is_connected,
        "tunnel_url": get_active_tunnel_url(),
    }


@app.get("/api/events")
async def api_events(request: Request, did: str = "", dtok: str = ""):
    dev_id, dev_tok = extract_device_creds(request, did, dtok)
    if not dev_id:
        raise HTTPException(status_code=400, detail="Missing device_id")

    dev = DEVICES_STORE.get(dev_id)
    if not dev:
        raise HTTPException(status_code=403, detail="Device not registered")

    status = dev.get("status", "pending")
    if status == "approved":
        stored_tok = str(dev.get("device_token") or "")
        if not (stored_tok and dev_tok and secrets.compare_digest(stored_tok, dev_tok)):
            raise HTTPException(status_code=403, detail="Invalid device credentials")

    dev["last_active"] = int(time.time())
    q: asyncio.Queue = asyncio.Queue(maxsize=32)
    device_subscribers.setdefault(dev_id, set()).add(q)

    async def event_stream():
        try:
            cur_dev = DEVICES_STORE.get(dev_id) or {}
            init_payload = json.dumps({
                "type": "init",
                "status": cur_dev.get("status", "pending"),
                "stopped": SYSTEM_STOPPED or bool(cur_dev.get("stopped", False)),
                "ts": int(time.time()),
            })
            yield f"data: {init_payload}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=12.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    d_ref = DEVICES_STORE.get(dev_id)
                    if d_ref:
                        d_ref["last_active"] = int(time.time())
                    yield ": keepalive\n\n"
        finally:
            subs = device_subscribers.get(dev_id)
            if subs:
                subs.discard(q)
                if not subs:
                    device_subscribers.pop(dev_id, None)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Protected Device-Specific Media Library & Streaming ---
def get_authorized_item(items: list[dict], index: int, device_id: str) -> dict:
    if index < 0 or index >= len(items):
        raise HTTPException(status_code=404, detail="Movie not found")
    item = items[index]
    allowed = item.get("allowed_devices") or []
    if device_id not in allowed:
        raise HTTPException(status_code=403, detail="Not shared with this device")
    return item


@app.get("/api/yplay/library")
async def yplay_library(request: Request, did: str = "", dtok: str = ""):
    dev = require_approved_device(request, did, dtok, check_stopped=False)
    dev_id = dev["device_id"]
    items = read_json(LIBRARY_FILE, [])
    enriched = []
    for idx, x in enumerate(items):
        allowed = x.get("allowed_devices") or []
        if dev_id not in allowed:
            continue
        row = dict(x)
        if not row.get("probed"):
            if block_path(row["id"], 0).exists():
                probed = await asyncio.to_thread(probe_cached_media_sync, row["id"])
                if probed:
                    update_library_probe(row["id"], probed)
                    row.update(probed)
            else:
                trigger_prewarm(row)
        row["index"] = idx
        row["quality"] = detect_quality(
            row.get("title", ""),
            int(row.get("width") or 0),
            int(row.get("height") or 0),
        )
        row.pop("allowed_devices", None)
        enriched.append(row)

    return {
        "stopped": SYSTEM_STOPPED or bool(dev.get("stopped", False)),
        "count": len(enriched),
        "items": enriched,
    }


@app.api_route("/api/yplay/stream/{index}", methods=["GET", "HEAD"])
async def yplay_stream(index: int, request: Request, did: str = "", dtok: str = ""):
    client_host = request.client.host if request.client else ""
    is_local_ffmpeg = client_host in ("127.0.0.1", "::1")

    items = read_json(LIBRARY_FILE, [])
    if is_local_ffmpeg:
        if index < 0 or index >= len(items):
            raise HTTPException(status_code=404, detail="Movie not found")
        item = items[index]
        dev = None
    else:
        dev = require_approved_device(request, did, dtok, check_stopped=True)
        item = get_authorized_item(items, index, dev["device_id"])

    item_id = item["id"]
    msg = await get_tg_message(item)
    media = msg.video or msg.document or msg.animation
    total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
    mime = item.get("mime_type") or getattr(media, "mime_type", None) or "video/mp4"

    if total_size <= 0:
        raise HTTPException(status_code=500, detail="Unknown media size")

    range_header = request.headers.get("range")
    start_byte = 0
    end_byte = total_size - 1

    if range_header and "=" in range_header:
        try:
            _, rng = range_header.strip().split("=", 1)
            s_part, e_part = rng.split("-", 1)
            if s_part:
                start_byte = max(0, int(s_part))
            if e_part:
                end_byte = min(total_size - 1, int(e_part))
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid range")

    if start_byte > end_byte or start_byte >= total_size:
        raise HTTPException(status_code=416, detail="Invalid range bounds")

    length = end_byte - start_byte + 1
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Cache-Control": "no-cache",
        "Content-Length": str(length),
    }
    status_code = 200
    if range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        status_code = 206

    if request.method == "HEAD":
        return Response(status_code=status_code, headers=headers)

    def is_aborted() -> bool:
        if SYSTEM_STOPPED:
            return True
        if dev and (dev.get("status") != "approved" or dev.get("stopped")):
            return True
        return False

    async def continuous_gen():
        pos = start_byte
        while pos <= end_byte:
            if is_aborted() or await request.is_disconnected():
                break

            b_idx = pos // CHUNK_SIZE
            b_path = block_path(item_id, b_idx)

            if b_path.exists():
                raw = await asyncio.to_thread(b_path.read_bytes)
                b_off = pos % CHUNK_SIZE
                if b_off < len(raw):
                    take = min(len(raw) - b_off, end_byte - pos + 1)
                    yield raw[b_off : b_off + take]
                    pos += take
                    continue

            stream_block = b_idx
            buf = bytearray()
            refresh_msg = await get_tg_message(item)

            try:
                async for chunk in tg_client.stream_media(refresh_msg, offset=b_idx):
                    if is_aborted() or await request.is_disconnected():
                        return
                    buf.extend(chunk)
                    while len(buf) >= CHUNK_SIZE:
                        blk_bytes = bytes(buf[:CHUNK_SIZE])
                        del buf[:CHUNK_SIZE]
                        await asyncio.to_thread(write_block_sync, block_path(item_id, stream_block), blk_bytes)

                        if pos <= end_byte:
                            cur_off = pos - (stream_block * CHUNK_SIZE)
                            if 0 <= cur_off < len(blk_bytes):
                                take = min(len(blk_bytes) - cur_off, end_byte - pos + 1)
                                yield blk_bytes[cur_off : cur_off + take]
                                pos += take

                        stream_block += 1
                        if pos > end_byte:
                            asyncio.create_task(asyncio.to_thread(prune_cache_sync))
                            return
                        if block_path(item_id, stream_block).exists():
                            break

                    if block_path(item_id, stream_block).exists():
                        break

                if buf and pos <= end_byte:
                    tail_bytes = bytes(buf)
                    await asyncio.to_thread(write_block_sync, block_path(item_id, stream_block), tail_bytes)
                    cur_off = pos - (stream_block * CHUNK_SIZE)
                    if 0 <= cur_off < len(tail_bytes):
                        take = min(len(tail_bytes) - cur_off, end_byte - pos + 1)
                        yield tail_bytes[cur_off : cur_off + take]
                        pos += take
            except Exception:
                msg_cache.pop(item_id, None)
                break

        asyncio.create_task(asyncio.to_thread(prune_cache_sync))

    return StreamingResponse(continuous_gen(), status_code=status_code, headers=headers, media_type=mime)


@app.api_route("/api/yplay/play/{index}", methods=["GET", "HEAD"])
@app.api_route("/api/yplay/compat/{index}", methods=["GET", "HEAD"])
async def yplay_play_stream(
    index: int,
    request: Request,
    t: float = 0.0,
    q: str = "orig",
    a: int = 0,
    did: str = "",
    dtok: str = "",
):
    global active_play_proc
    dev = require_approved_device(request, did, dtok, check_stopped=True)
    items = read_json(LIBRARY_FILE, [])
    get_authorized_item(items, index, dev["device_id"])

    if request.method == "HEAD":
        return Response(status_code=200, headers={"Content-Type": "video/mp4", "Cache-Control": "no-store"})

    await kill_active_play_proc()

    seek_sec = max(0.0, float(t or 0.0))
    audio_idx = max(0, int(a or 0))
    q_clean = str(q or "orig").lower()
    is_transcode = q_clean in ("720", "480", "360")

    cmd = ["ffmpeg", "-nostdin", "-loglevel", "error"]
    if seek_sec > 0.1:
        if not is_transcode:
            cmd.append("-noaccurate_seek")
        cmd.extend(["-ss", f"{seek_sec:.2f}"])

    cmd.extend([
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{index}",
        "-map", "0:v:0", "-map", f"0:a:{audio_idx}?",
    ])

    if is_transcode:
        h = int(q_clean)
        rate = "1600k" if h == 720 else ("850k" if h == 480 else "480k")
        buf = "3200k" if h == 720 else ("1700k" if h == 480 else "960k")
        cmd.extend([
            "-vf", f"scale=-2:{h}",
            "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
            "-crf", "26", "-maxrate", rate, "-bufsize", buf,
            "-pix_fmt", "yuv420p", "-fps_mode", "cfr", "-threads", "2",
        ])
    else:
        cmd.extend(["-c:v", "copy"])

    cmd.extend([
        "-c:a", "aac", "-ac", "2", "-b:a", "128k",
        "-af", "aresample=async=1000",
        "-avoid_negative_ts", "make_zero",
        "-f", "mp4",
        "-movflags", "frag_keyframe+empty_moov+default_base_moof+negative_cts_offsets",
        "pipe:1",
    ])

    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.DEVNULL,
    )
    active_play_proc = proc

    async def remux_gen():
        global active_play_proc
        try:
            while True:
                if SYSTEM_STOPPED or dev.get("status") != "approved" or dev.get("stopped"):
                    break
                if await request.is_disconnected():
                    break
                chunk = await proc.stdout.read(65536)
                if not chunk:
                    break
                yield chunk
        finally:
            if proc.returncode is None:
                try:
                    proc.kill()
                    await proc.wait()
                except Exception:
                    pass
            if active_play_proc is proc:
                active_play_proc = None

    return StreamingResponse(
        remux_gen(),
        status_code=200,
        media_type="video/mp4",
        headers={"Cache-Control": "no-store"},
    )


@app.get("/api/yplay/sub/{index}/{sub_idx}.vtt")
async def yplay_sub_vtt(
    index: int,
    sub_idx: int,
    request: Request,
    t: float = 0.0,
    dur: float = 180.0,
    did: str = "",
    dtok: str = "",
):
    dev = require_approved_device(request, did, dtok, check_stopped=True)
    items = read_json(LIBRARY_FILE, [])
    get_authorized_item(items, index, dev["device_id"])

    start_sec = max(0.0, float(t or 0.0))
    win_sec = min(240.0, max(30.0, float(dur or 180.0)))
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error",
        "-ss", f"{start_sec:.2f}",
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{index}",
        "-t", f"{win_sec:.2f}",
        "-vn", "-an",
        "-map", f"0:s:{max(0, sub_idx)}",
        "-f", "webvtt",
        "pipe:1",
    ]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL,
        )
        out, _ = await asyncio.wait_for(proc.communicate(), timeout=15)
        vtt = out.decode("utf-8", errors="ignore") if out else "WEBVTT\n\n"
    except Exception:
        vtt = "WEBVTT\n\n"

    return Response(content=vtt, media_type="text/vtt", headers={"Cache-Control": "no-store"})


@app.get("/live/suntv/index.m3u8")
async def suntv_m3u8(request: Request, did: str = "", dtok: str = ""):
    require_approved_device(request, did, dtok, check_stopped=True)
    ok = await start_suntv_relay()
    m3u8 = HLS_DIR / "index.m3u8"
    if not ok or not m3u8.exists():
        raise HTTPException(status_code=503, detail=last_error or "Sun TV relay not ready")
    return FileResponse(str(m3u8), media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store"})


@app.get("/live/suntv/{seg}")
async def suntv_ts(seg: str, request: Request, did: str = "", dtok: str = ""):
    global last_relay_hit
    require_approved_device(request, did, dtok, check_stopped=True)
    last_relay_hit = time.time()
    if "/" in seg or ".." in seg or not seg.endswith(".ts"):
        raise HTTPException(status_code=400, detail="Bad segment")
    p = HLS_DIR / seg
    if not p.exists():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(str(p), media_type="video/mp2t", headers={"Cache-Control": "no-cache"})
