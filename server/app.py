import asyncio
import html
import json
import os
import re
import shutil
import subprocess
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import Body, FastAPI, HTTPException, Request
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
DEVICES_FILE = DATA_DIR / "devices.json"
PENDING_FILE = DATA_DIR / "pending_forwards.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
OWNER_FILE = DATA_DIR / "owner.json"
STATE_FILE = DATA_DIR / "system_state.json"
SESSION_FILE = DATA_DIR / "ystream_bot"

CHUNK_SIZE = 1024 * 1024  # 1 MiB
MAX_CACHE_BYTES = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB cap
ONLINE_WINDOW_SEC = 28
MAX_PENDING_DEVICES = 15
RELAY_IDLE_SEC = 45
START_TIME = time.time()
NATIVE_AUDIO_CODECS = {"aac", "mp3", "opus", "vorbis", "flac"}
TEXT_SUB_CODECS = {"subrip", "srt", "ass", "ssa", "mov_text", "webvtt", "text"}

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
device_subscribers: dict[str, set[asyncio.Queue]] = {}
active_sse_counts: dict[str, int] = {}
approval_notify_ts: dict[str, float] = {}


def read_json(path: Path, default):
    if not path.exists():
        path.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, data):
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)


read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
read_json(DEVICES_FILE, {"selected_device_id": "", "devices": {}})
read_json(PENDING_FILE, {})
SYSTEM_STOPPED: bool = bool(read_json(STATE_FILE, {"stopped": False}).get("stopped", False))


def get_devices_store() -> dict:
    raw = read_json(DEVICES_FILE, {"selected_device_id": "", "devices": {}})
    if not isinstance(raw, dict):
        raw = {"selected_device_id": "", "devices": {}}
    if "devices" not in raw or not isinstance(raw["devices"], dict):
        raw["devices"] = {}
    if "selected_device_id" not in raw:
        raw["selected_device_id"] = ""
    return raw


def save_devices_store(store: dict):
    write_json(DEVICES_FILE, store)


def clean_device_id(raw: str) -> str:
    val = re.sub(r"[^a-zA-Z0-9_-]", "", str(raw or "").strip())
    return val[:64] if len(val) >= 6 else ""


def touch_device(device_id: str):
    did = clean_device_id(device_id)
    if not did:
        return
    store = get_devices_store()
    dev = store["devices"].get(did)
    if dev:
        dev["last_seen"] = int(time.time())
        save_devices_store(store)


def is_device_online(did: str, dev: dict) -> bool:
    if active_sse_counts.get(did, 0) > 0:
        return True
    last_seen = int(dev.get("last_seen") or 0)
    return (time.time() - last_seen) <= ONLINE_WINDOW_SEC


def fmt_last_active(did: str, dev: dict) -> str:
    if is_device_online(did, dev):
        return "Online now"
    last_seen = int(dev.get("last_seen") or 0)
    if last_seen <= 0:
        return "Never"
    diff = max(1, int(time.time() - last_seen))
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h {(diff % 3600) // 60}m ago"
    return f"{diff // 86400}d ago"


def push_device_event(device_id: str, payload_obj: dict):
    subs = device_subscribers.get(device_id)
    if not subs:
        return
    payload = json.dumps(payload_obj)
    for q in list(subs):
        try:
            q.put_nowait(payload)
        except Exception:
            pass


def push_all_devices_event(payload_obj: dict):
    payload = json.dumps(payload_obj)
    for subs in list(device_subscribers.values()):
        for q in list(subs):
            try:
                q.put_nowait(payload)
            except Exception:
                pass


def require_approved_device(request: Request, device_id: str = "") -> tuple[str, dict]:
    if request.query_params.get("internal") == "1":
        client_host = request.client.host if request.client else ""
        if client_host in ("127.0.0.1", "::1", "localhost"):
            return "internal", {"status": "approved"}

    did = clean_device_id(device_id or request.query_params.get("device_id") or request.headers.get("x-device-id", ""))
    if not did:
        raise HTTPException(status_code=403, detail="Device ID required")

    store = get_devices_store()
    dev = store["devices"].get(did)
    if not dev or dev.get("status") != "approved":
        raise HTTPException(status_code=403, detail="Device not approved")

    dev["last_seen"] = int(time.time())
    save_devices_store(store)
    return did, dev


def find_media_item(item_id: str, device_id: str = "") -> Optional[dict]:
    store = get_devices_store()
    if device_id and device_id in store["devices"]:
        for m in store["devices"][device_id].get("movies", []):
            if m.get("id") == item_id:
                return m
    for dev in store["devices"].values():
        for m in dev.get("movies", []):
            if m.get("id") == item_id:
                return m
    return None


def is_item_referenced_anywhere(item_id: str) -> bool:
    store = get_devices_store()
    for dev in store["devices"].values():
        for m in dev.get("movies", []):
            if m.get("id") == item_id:
                return True
    pending = read_json(PENDING_FILE, {})
    for row in pending.values():
        if isinstance(row, dict) and row.get("id") == item_id:
            return True
    return False


def set_system_stopped(stopped: bool):
    global SYSTEM_STOPPED
    SYSTEM_STOPPED = bool(stopped)
    write_json(STATE_FILE, {"stopped": SYSTEM_STOPPED, "updated_at": int(time.time())})
    push_all_devices_event({"type": "system_state", "stopped": SYSTEM_STOPPED, "ts": int(time.time())})


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
    duration = int(getattr(media, "duration", 0) or 0)
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
        "duration": duration,
        "width": width,
        "height": height,
        "quality": detect_quality(title, width, height),
        "audio_codec": "",
        "compat_audio": likely_compat,
        "audio_tracks": [],
        "default_audio_idx": 0,
        "subtitles": [],
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


def delete_item_cache_sync(item_id: str):
    safe = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
    p = CACHE_DIR / safe
    if p.exists():
        shutil.rmtree(p, ignore_errors=True)


def get_cache_stats_sync() -> tuple[int, int]:
    files = [f for f in CACHE_DIR.glob("*/*.blk") if f.exists()]
    return len(files), sum(f.stat().st_size for f in files)


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


def run_ffprobe_json(target: str, analyze_us: int = 4000000, timeout_sec: int = 9) -> Optional[dict]:
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-analyzeduration", str(analyze_us),
            "-probesize", str(analyze_us),
            "-show_entries", "format=duration:stream=codec_type,codec_name,width,height,duration:stream_tags=language,title,DURATION",
            "-of", "json", target,
        ]
        res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL, timeout=timeout_sec)
        return json.loads(res)
    except Exception:
        return None


def parse_hms_duration(val: str) -> int:
    try:
        parts = str(val or "").strip().split(":")
        if len(parts) == 3:
            return int(float(parts[0]) * 3600 + float(parts[1]) * 60 + float(parts[2]))
    except Exception:
        pass
    return 0


def parse_ffprobe_data(data: dict, allow_duration: bool = True, file_size: int = 0) -> Optional[dict]:
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
    if allow_duration:
        candidates = []
        try:
            fd = float(fmt_info.get("duration") or 0)
            if fd > 0:
                candidates.append(int(fd))
        except Exception:
            pass
        for s in streams:
            try:
                sd = float(s.get("duration") or 0)
                if sd > 0:
                    candidates.append(int(sd))
            except Exception:
                pass
            tags = s.get("tags") or {}
            td = parse_hms_duration(tags.get("DURATION") or tags.get("duration") or "")
            if td > 0:
                candidates.append(td)

        if candidates:
            best_dur = max(candidates)
            # Reject truncated-buffer 9s artifacts on large movie files (>25 MB)
            if not (file_size > 25 * 1024 * 1024 and best_dur < 45):
                dur = best_dur

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


def probe_media_sync(item_id: str, total_size: int = 0, existing_dur: int = 0) -> Optional[dict]:
    parsed_tracks = None
    b0 = block_path(item_id, 0)
    if b0.exists():
        b1 = block_path(item_id, 1)
        tmp_probe = safe_item_dir(item_id) / "probe.tmp"
        try:
            with tmp_probe.open("wb") as out:
                out.write(b0.read_bytes())
                if b1.exists():
                    out.write(b1.read_bytes())
            # NEVER read duration from truncated 2MB probe.tmp (fixes 9-second bug!)
            raw_head = run_ffprobe_json(str(tmp_probe), analyze_us=2500000, timeout_sec=6)
            if raw_head:
                parsed_tracks = parse_ffprobe_data(raw_head, allow_duration=False, file_size=total_size)
        finally:
            try:
                tmp_probe.unlink()
            except Exception:
                pass

    # Probe full HTTP range stream to get exact container duration + tracks if needed
    need_full_probe = (existing_dur <= 0) or (parsed_tracks is None)
    if need_full_probe:
        raw_full = run_ffprobe_json(
            f"http://127.0.0.1:8000/api/yplay/stream/{item_id}?internal=1",
            analyze_us=8000000,
            timeout_sec=12,
        )
        if raw_full:
            parsed_full = parse_ffprobe_data(raw_full, allow_duration=True, file_size=total_size)
            if parsed_full:
                if parsed_tracks:
                    if not parsed_full["audio_tracks"] and parsed_tracks["audio_tracks"]:
                        parsed_full["audio_tracks"] = parsed_tracks["audio_tracks"]
                        parsed_full["default_audio_idx"] = parsed_tracks["default_audio_idx"]
                        parsed_full["audio_codec"] = parsed_tracks["audio_codec"]
                        parsed_full["compat_audio"] = parsed_tracks["compat_audio"]
                    if not parsed_full["subtitles"] and parsed_tracks["subtitles"]:
                        parsed_full["subtitles"] = parsed_tracks["subtitles"]
                    if not parsed_full["width"] and parsed_tracks["width"]:
                        parsed_full["width"] = parsed_tracks["width"]
                        parsed_full["height"] = parsed_tracks["height"]
                return parsed_full

    return parsed_tracks


def update_device_movie_probe(device_id: str, item_id: str, info: dict):
    store = get_devices_store()
    dev = store["devices"].get(device_id)
    if not dev:
        return
    changed = False
    for row in dev.get("movies", []):
        if row.get("id") == item_id:
            row["probed"] = True
            if info.get("audio_tracks"):
                row["audio_tracks"] = info["audio_tracks"]
                row["default_audio_idx"] = int(info.get("default_audio_idx", 0))
            if info.get("subtitles"):
                row["subtitles"] = info["subtitles"]
            if info.get("audio_codec"):
                row["audio_codec"] = info["audio_codec"]
                row["compat_audio"] = bool(info["compat_audio"])
            if info.get("width") and not row.get("width"):
                row["width"] = info["width"]
            if info.get("height") and not row.get("height"):
                row["height"] = info["height"]
            if info.get("duration") and int(info["duration"]) > 0:
                row["duration"] = int(info["duration"])
            row["quality"] = detect_quality(
                row.get("title", ""),
                int(row.get("width") or 0),
                int(row.get("height") or 0),
            )
            changed = True
            break
    if changed:
        save_devices_store(store)
        push_device_event(device_id, {"type": "library_updated", "item_id": item_id})


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


async def get_tg_message(item: dict, force_refresh: bool = False) -> Message:
    item_id = item["id"]
    if not force_refresh and item_id in msg_cache:
        return msg_cache[item_id]
    if len(msg_cache) > 50:
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


async def prewarm_for_device(device_id: str, item: dict):
    item_id = item["id"]
    task_key = f"{device_id}:{item_id}"
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

        probe_res = await asyncio.to_thread(
            probe_media_sync,
            item_id,
            total_size,
            int(item.get("duration") or 0),
        )
        if probe_res:
            update_device_movie_probe(device_id, item_id, probe_res)

        await asyncio.to_thread(prune_cache_sync)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[Prewarm {task_key}] {exc}")
    finally:
        prewarm_tasks.pop(task_key, None)


def trigger_prewarm(device_id: str, item: dict):
    task_key = f"{device_id}:{item['id']}"
    if task_key in prewarm_tasks:
        return
    if block_path(item["id"], 0).exists() and item.get("probed") and int(item.get("duration") or 0) > 0:
        return
    prewarm_tasks[task_key] = asyncio.create_task(prewarm_for_device(device_id, item))


def add_movie_to_device(device_id: str, item: dict) -> tuple[bool, int, str]:
    store = get_devices_store()
    dev = store["devices"].get(device_id)
    if not dev or dev.get("status") != "approved":
        return False, 0, ""

    movies = dev.setdefault("movies", [])
    for row in movies:
        if row.get("id") == item["id"]:
            trigger_prewarm(device_id, row)
            push_device_event(device_id, {"type": "library_updated", "item_id": item["id"]})
            return False, len(movies), dev.get("name", device_id)

    movie_copy = json.loads(json.dumps(item))
    movie_copy["added_at"] = int(time.time())
    movies.append(movie_copy)
    save_devices_store(store)
    trigger_prewarm(device_id, movie_copy)
    push_device_event(device_id, {"type": "library_updated", "item_id": movie_copy["id"]})
    return True, len(movies), dev.get("name", device_id)


# --- Telegram Bot UI Builders ---
def build_device_dashboard() -> tuple[str, InlineKeyboardMarkup]:
    store = get_devices_store()
    selected_id = store.get("selected_device_id", "")
    devices = store.get("devices", {})

    if not devices:
        text = (
            "<b>📱 YStream Device Manager</b>\n\n"
            "No devices registered yet.\n"
            "Open YStream on your TV or browser to trigger an instant approval request."
        )
        kb = InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="dev:list")]])
        return text, kb

    sel_dev = devices.get(selected_id) if selected_id else None
    if sel_dev and sel_dev.get("status") == "approved":
        target_line = f"🎯 <b>Active Target:</b> <code>{html.escape(sel_dev.get('name', selected_id))}</code>"
    else:
        target_line = "🎯 <b>Active Target:</b> <i>None (Bot asks on movie forward)</i>"

    lines = ["<b>📱 YStream Device Manager</b>", target_line, ""]
    buttons = []

    sorted_devs = sorted(
        devices.items(),
        key=lambda kv: (0 if kv[1].get("status") == "approved" else 1, -int(kv[1].get("last_seen") or 0)),
    )

    for idx, (did, dev) in enumerate(sorted_devs, start=1):
        status = dev.get("status", "pending")
        online = is_device_online(did, dev)
        state_dot = "🟢" if (status == "approved" and online) else ("⏳" if status == "pending" else ("🚫" if status == "revoked" else "⚫"))
        status_lbl = "Online" if (status == "approved" and online) else ("Pending Approval" if status == "pending" else ("Removed / Revoked" if status == "revoked" else "Offline"))
        is_target = (did == selected_id and status == "approved")
        target_tag = " • 🎯 TARGET" if is_target else ""
        q_count = len(dev.get("movies", []))

        lines.append(
            f"{state_dot} <b>{idx}. {html.escape(dev.get('name', did))}</b>{target_tag}\n"
            f"   ID: <code>{html.escape(did)}</code> • {status_lbl}\n"
            f"   Last active: <code>{fmt_last_active(did, dev)}</code> • Queue: <code>{q_count}</code>"
        )

        row_btns = []
        if status == "approved":
            sel_label = f"✅ Target: {dev.get('name', did)[:14]}" if is_target else f"🎯 Select {dev.get('name', did)[:14]}"
            row_btns.append(InlineKeyboardButton(sel_label, callback_data=f"dev:select:{did}"))
            row_btns.append(InlineKeyboardButton("⚙️ Manage", callback_data=f"dev:manage:{did}"))
        else:
            row_btns.append(InlineKeyboardButton(f"✅ Approve {dev.get('name', did)[:14]}", callback_data=f"dev:approve:{did}"))
            row_btns.append(InlineKeyboardButton("⚙️ Manage", callback_data=f"dev:manage:{did}"))
        buttons.append(row_btns)

    footer_row = [InlineKeyboardButton("🔄 Refresh", callback_data="dev:list")]
    if sel_dev and sel_dev.get("status") == "approved":
        footer_row.append(InlineKeyboardButton("✖ Unselect Target", callback_data="dev:unselect"))
    buttons.append(footer_row)

    return "\n".join(lines), InlineKeyboardMarkup(buttons)


def build_single_device_panel(did: str) -> tuple[str, InlineKeyboardMarkup]:
    store = get_devices_store()
    dev = store["devices"].get(did)
    if not dev:
        return build_device_dashboard()

    selected_id = store.get("selected_device_id", "")
    status = dev.get("status", "pending")
    online = is_device_online(did, dev)
    is_target = (did == selected_id and status == "approved")
    movies = dev.get("movies", [])

    movie_lines = ""
    if movies:
        top_m = [f"   {i+1}. {html.escape(m.get('title', 'Movie')[:34])}" for i, m in enumerate(movies[:5])]
        if len(movies) > 5:
            top_m.append(f"   <i>...and {len(movies) - 5} more</i>")
        movie_lines = "\n\n<b>🎬 Device Queue:</b>\n" + "\n".join(top_m)

    text = (
        f"<b>⚙️ Device Settings</b>\n\n"
        f"<b>Name:</b> {html.escape(dev.get('name', did))}\n"
        f"<b>Device ID:</b> <code>{html.escape(did)}</code>\n"
        f"<b>Platform:</b> {html.escape(dev.get('platform', 'Unknown'))}\n"
        f"<b>Access:</b> <code>{status.upper()}</code>\n"
        f"<b>Connection:</b> <code>{'ONLINE' if online else 'OFFLINE'}</code>\n"
        f"<b>Last Active:</b> <code>{fmt_last_active(did, dev)}</code>\n"
        f"<b>Movie Target:</b> <code>{'YES' if is_target else 'NO'}</code>"
        f"{movie_lines}"
    )

    rows = []
    if status == "approved":
        if not is_target:
            rows.append([InlineKeyboardButton("🎯 Select as Movie Target", callback_data=f"dev:select:{did}")])
        if movies:
            rows.append([InlineKeyboardButton(f"🗑 Clear Device Movies ({len(movies)})", callback_data=f"dev:clear:{did}")])
        rows.append([InlineKeyboardButton("🚫 Remove & Revoke Access", callback_data=f"dev:revoke:{did}")])
    else:
        rows.append([InlineKeyboardButton("✅ Approve / Re-Approve Access", callback_data=f"dev:approve:{did}")])
        rows.append([InlineKeyboardButton("🗑 Delete Record Permanently", callback_data=f"dev:delete:{did}")])

    rows.append([
        InlineKeyboardButton("🔄 Refresh", callback_data=f"dev:manage:{did}"),
        InlineKeyboardButton("⬅️ All Devices", callback_data="dev:list"),
    ])
    return text, InlineKeyboardMarkup(rows)


async def build_status_html() -> str:
    store = get_devices_store()
    devices = store.get("devices", {})
    approved_cnt = sum(1 for d in devices.values() if d.get("status") == "approved")
    online_cnt = sum(1 for did, d in devices.items() if d.get("status") == "approved" and is_device_online(did, d))
    total_movies = sum(len(d.get("movies", [])) for d in devices.values())

    blk_cnt, blk_bytes = await asyncio.to_thread(get_cache_stats_sync)
    sys = await asyncio.to_thread(get_sys_stats)
    tunnel = get_active_tunnel_url()
    owner_id = get_owner_id()

    rows = [
        ("Bot", "Connected" if tg_client.is_connected else "Offline"),
        ("System", "SHUTDOWN" if SYSTEM_STOPPED else "ACTIVE"),
        ("Owner Lock", str(owner_id) if owner_id else "Unlocked"),
        ("Devices", f"{online_cnt} online / {approved_cnt} ok"),
        ("Queues", f"{total_movies} movie(s)"),
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

    out = f"<b>📊 YStream System Diagnosis</b>\n<pre>{html.escape(table)}</pre>"
    if tunnel:
        out += f"\n\n<b>Tunnel URL:</b>\n<code>{html.escape(tunnel)}</code>"
    return out


async def notify_owner_device_request(did: str, dev: dict, is_reaccess: bool = False):
    owner_id = get_owner_id()
    if owner_id <= 0 or not tg_client.is_connected:
        return
    now = time.time()
    if now - approval_notify_ts.get(did, 0.0) < 25.0:
        return
    approval_notify_ts[did] = now

    header = "🔄 <b>Device Re-Access Request</b>" if is_reaccess else "🔔 <b>New Device Access Request</b>"
    text = (
        f"{header}\n\n"
        f"<b>Device:</b> {html.escape(dev.get('name', did))}\n"
        f"<b>Platform:</b> {html.escape(dev.get('platform', 'Unknown'))}\n"
        f"<b>Device ID:</b> <code>{html.escape(did)}</code>\n\n"
        f"Approve permanent access for this device?"
    )
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Approve", callback_data=f"dev:approve:{did}"),
        InlineKeyboardButton("❌ Cancel", callback_data=f"dev:reject:{did}"),
    ]])
    try:
        await tg_client.send_message(owner_id, text, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception as exc:
        print(f"[Notify owner error] {exc}")


# --- Owner-Only Telegram Bot Commands ---
@tg_client.on_message(filters.private & filters.command("start"))
async def cmd_start(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    state_txt = "🛑 SYSTEM SHUTDOWN" if SYSTEM_STOPPED else "🟢 ACTIVE"
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📱 Devices", callback_data="dev:list"),
            InlineKeyboardButton("📊 Status", callback_data="cb:status"),
        ],
        [
            InlineKeyboardButton(
                "▶️ Resume System" if SYSTEM_STOPPED else "🛑 System Shutdown",
                callback_data="sys:resume" if SYSTEM_STOPPED else "sys:shutdown",
            )
        ],
    ])
    await msg.reply_text(
        f"<b>🎬 YStream Control Center</b>\n\n"
        f"<b>System State:</b> <code>{state_txt}</code>\n"
        f"• Use /device to manage & select TV/Mobile devices\n"
        f"• Forward any movie to send to the selected device\n"
        f"• Use /shutdown & /resume for instant emergency control",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=kb,
    )


@tg_client.on_message(filters.private & filters.command(["device", "devices"]))
async def cmd_device(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    txt, kb = build_device_dashboard()
    await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


@tg_client.on_message(filters.private & filters.command("shutdown"))
async def cmd_shutdown(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    set_system_stopped(True)
    await kill_active_play_proc()
    stop_suntv_relay()
    kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Resume Playback", callback_data="sys:resume")]])
    await msg.reply_text(
        "<b>🛑 System Shutdown Activated</b>\n\n"
        "All TV & Browser screens are now locked with the System Shutdown alert.",
        parse_mode=enums.ParseMode.HTML,
        reply_markup=kb,
    )


@tg_client.on_message(filters.private & filters.command(["resume", "poweron"]))
async def cmd_resume(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    set_system_stopped(False)
    await msg.reply_text(
        "<b>▶️ System Resumed</b>\n\nAll approved devices have resumed playback automatically.",
        parse_mode=enums.ParseMode.HTML,
    )


@tg_client.on_message(filters.private & filters.command("status"))
async def cmd_status(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=True):
        return
    txt = await build_status_html()
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Refresh", callback_data="cb:status"),
        InlineKeyboardButton("📱 Devices", callback_data="dev:list"),
    ]])
    await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)


@tg_client.on_message(filters.private & filters.command("clear"))
async def cmd_clear(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    store = get_devices_store()
    sel_id = store.get("selected_device_id", "")
    if sel_id and sel_id in store["devices"]:
        dev = store["devices"][sel_id]
        old_ids = [m.get("id") for m in dev.get("movies", []) if m.get("id")]
        dev["movies"] = []
        dev["progress"] = {}
        save_devices_store(store)
        for oid in old_ids:
            if not is_item_referenced_anywhere(oid):
                await asyncio.to_thread(delete_item_cache_sync, oid)
        push_device_event(sel_id, {"type": "library_updated"})
        await msg.reply_text(
            f"<b>🗑 Cleared Queue for {html.escape(dev.get('name', sel_id))}</b>",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        txt, kb = build_device_dashboard()
        await msg.reply_text(
            "<b>⚠️ Select a device first to clear its queue:</b>\n\n" + txt,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=kb,
        )


@tg_client.on_message(filters.private & (filters.video | filters.document | filters.animation))
async def on_file(_, msg: Message):
    if not authorize_user(msg.from_user, allow_claim=False):
        return
    info = get_media_info(msg)
    if not info:
        return

    store = get_devices_store()
    sel_id = store.get("selected_device_id", "")
    sel_dev = store["devices"].get(sel_id) if sel_id else None

    # Never add a movie without a valid selected approved device!
    if not sel_dev or sel_dev.get("status") != "approved":
        approved_devs = [
            (did, d) for did, d in store["devices"].items() if d.get("status") == "approved"
        ]
        if not approved_devs:
            await msg.reply_text(
                "<b>⚠️ No Approved Devices Found</b>\n\n"
                "Open YStream on your TV or browser and approve the device first.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        token = uuid.uuid4().hex[:10]
        pending = read_json(PENDING_FILE, {})
        if len(pending) > 25:
            oldest = list(pending.keys())[:10]
            for k in oldest:
                pending.pop(k, None)
        pending[token] = info
        write_json(PENDING_FILE, pending)

        btn_rows = []
        for did, d in approved_devs:
            online_dot = "🟢" if is_device_online(did, d) else "⚫"
            btn_rows.append([
                InlineKeyboardButton(
                    f"{online_dot} Send to {d.get('name', did)[:24]}",
                    callback_data=f"fwd:{token}:{did}",
                )
            ])
        btn_rows.append([InlineKeyboardButton("❌ Cancel", callback_data=f"fwdcancel:{token}")])

        await msg.reply_text(
            f"<b>🎯 Select Target Device</b>\n\n"
            f"<b>Movie:</b> {html.escape(info['title'])}\n"
            f"<b>Quality:</b> <code>{html.escape(info['quality'])}</code> • <b>Size:</b> <code>{fmt_bytes(info['file_size'])}</code>\n\n"
            f"<i>No device is selected. Choose which device should receive this movie:</i>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(btn_rows),
        )
        return

    added, total, dev_name = add_movie_to_device(sel_id, info)
    kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("🔄 Switch Device", callback_data="dev:list"),
        InlineKeyboardButton("⚙️ Manage Queue", callback_data=f"dev:manage:{sel_id}"),
    ]])
    if added:
        await msg.reply_text(
            f"<b>✅ Delivered to {html.escape(dev_name)}</b>\n\n"
            f"<b>Slot:</b> <code>#{total}</code> • <code>{html.escape(info['quality'])}</code> • <code>{fmt_bytes(info['file_size'])}</code>\n"
            f"<b>Title:</b> {html.escape(info['title'])}",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=kb,
        )
    else:
        await msg.reply_text(
            f"<b>ℹ️ Already in {html.escape(dev_name)}'s Queue</b>\n<b>{html.escape(info['title'])}</b>",
            parse_mode=enums.ParseMode.HTML,
            reply_markup=kb,
        )


@tg_client.on_callback_query()
async def on_callback(_, cq: CallbackQuery):
    if not authorize_user(cq.from_user, allow_claim=False):
        return
    data = str(cq.data or "")

    try:
        if data == "cb:status":
            await cq.answer("Refreshing...")
            txt = await build_status_html()
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Refresh", callback_data="cb:status"),
                InlineKeyboardButton("📱 Devices", callback_data="dev:list"),
            ]])
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data == "sys:shutdown":
            set_system_stopped(True)
            await kill_active_play_proc()
            stop_suntv_relay()
            await cq.answer("System Shutdown Activated")
            kb = InlineKeyboardMarkup([[InlineKeyboardButton("▶️ Resume Playback", callback_data="sys:resume")]])
            await cq.message.edit_text(
                "<b>🛑 System Shutdown Activated</b>\n\nAll TV & Browser screens are now locked.",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=kb,
            )
            return

        if data == "sys:resume":
            set_system_stopped(False)
            await cq.answer("System Resumed")
            await cq.message.edit_text(
                "<b>▶️ System Resumed</b>\n\nPlayback restored across all approved devices.",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data == "dev:list":
            await cq.answer()
            txt, kb = build_device_dashboard()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data == "dev:unselect":
            store = get_devices_store()
            store["selected_device_id"] = ""
            save_devices_store(store)
            await cq.answer("Target cleared")
            txt, kb = build_device_dashboard()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:select:"):
            did = data.split(":", 2)[2]
            store = get_devices_store()
            dev = store["devices"].get(did)
            if not dev or dev.get("status") != "approved":
                await cq.answer("Device must be approved first", show_alert=True)
                return
            store["selected_device_id"] = did
            save_devices_store(store)
            await cq.answer(f"Selected: {dev.get('name', did)}")
            txt, kb = build_device_dashboard()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:manage:"):
            did = data.split(":", 2)[2]
            await cq.answer()
            txt, kb = build_single_device_panel(did)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:approve:"):
            did = data.split(":", 2)[2]
            store = get_devices_store()
            dev = store["devices"].get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            dev["status"] = "approved"
            dev["approved_at"] = int(time.time())
            dev["last_seen"] = int(time.time())
            if not store.get("selected_device_id"):
                store["selected_device_id"] = did
            save_devices_store(store)
            push_device_event(did, {"type": "device_status", "status": "approved", "device_name": dev.get("name", did)})
            await cq.answer("Device Approved!")
            txt, kb = build_single_device_panel(did)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:reject:") or data.startswith("dev:revoke:"):
            did = data.split(":", 2)[2]
            store = get_devices_store()
            dev = store["devices"].get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            dev["status"] = "revoked"
            if store.get("selected_device_id") == did:
                store["selected_device_id"] = ""
            save_devices_store(store)
            push_device_event(did, {"type": "device_status", "status": "revoked"})
            await cq.answer("Access Revoked Immediately")
            txt, kb = build_single_device_panel(did)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:clear:"):
            did = data.split(":", 2)[2]
            store = get_devices_store()
            dev = store["devices"].get(did)
            if not dev:
                await cq.answer("Device not found", show_alert=True)
                return
            old_ids = [m.get("id") for m in dev.get("movies", []) if m.get("id")]
            dev["movies"] = []
            dev["progress"] = {}
            save_devices_store(store)
            for oid in old_ids:
                if not is_item_referenced_anywhere(oid):
                    await asyncio.to_thread(delete_item_cache_sync, oid)
            push_device_event(did, {"type": "library_updated"})
            await cq.answer("Device queue cleared")
            txt, kb = build_single_device_panel(did)
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("dev:delete:"):
            did = data.split(":", 2)[2]
            store = get_devices_store()
            dev = store["devices"].pop(did, None)
            if store.get("selected_device_id") == did:
                store["selected_device_id"] = ""
            save_devices_store(store)
            if dev:
                for m in dev.get("movies", []):
                    oid = m.get("id")
                    if oid and not is_item_referenced_anywhere(oid):
                        await asyncio.to_thread(delete_item_cache_sync, oid)
            push_device_event(did, {"type": "device_status", "status": "revoked"})
            await cq.answer("Device deleted")
            txt, kb = build_device_dashboard()
            await cq.message.edit_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
            return

        if data.startswith("fwdcancel:"):
            token = data.split(":", 1)[1]
            pending = read_json(PENDING_FILE, {})
            info = pending.pop(token, None)
            write_json(PENDING_FILE, pending)
            if info and info.get("id") and not is_item_referenced_anywhere(info["id"]):
                await asyncio.to_thread(delete_item_cache_sync, info["id"])
            await cq.answer("Cancelled")
            await cq.message.edit_text("<b>❌ Movie Delivery Cancelled</b>", parse_mode=enums.ParseMode.HTML)
            return

        if data.startswith("fwd:"):
            _, token, did = data.split(":", 2)
            pending = read_json(PENDING_FILE, {})
            info = pending.pop(token, None)
            write_json(PENDING_FILE, pending)
            if not info:
                await cq.answer("Request expired", show_alert=True)
                return
            store = get_devices_store()
            dev = store["devices"].get(did)
            if not dev or dev.get("status") != "approved":
                await cq.answer("Device not approved", show_alert=True)
                return
            store["selected_device_id"] = did
            save_devices_store(store)
            added, total, dev_name = add_movie_to_device(did, info)
            await cq.answer(f"Sent to {dev_name}")
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("🔄 Switch Device", callback_data="dev:list"),
                InlineKeyboardButton("⚙️ Manage Queue", callback_data=f"dev:manage:{did}"),
            ]])
            await cq.message.edit_text(
                f"<b>✅ Delivered to {html.escape(dev_name)}</b>\n\n"
                f"<b>Slot:</b> <code>#{total}</code> • <code>{html.escape(info['quality'])}</code> • <code>{fmt_bytes(info['file_size'])}</code>\n"
                f"<b>Title:</b> {html.escape(info['title'])}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=kb,
            )
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
            BotCommand("device", "Manage & Select Devices"),
            BotCommand("status", "System Diagnosis Table"),
            BotCommand("shutdown", "Emergency System Shutdown"),
            BotCommand("resume", "Resume System Playback"),
            BotCommand("clear", "Clear Selected Device Queue"),
        ])
    except Exception as exc:
        print(f"[Bot commands error] {exc}")

    store = get_devices_store()
    for did, dev in store.get("devices", {}).items():
        if dev.get("status") == "approved" and dev.get("movies"):
            trigger_prewarm(did, dev["movies"][0])
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


# --- Device Registration, Approval & Real-Time SSE Endpoints ---
@app.post("/api/device/register")
async def api_device_register(payload: dict = Body(...)):
    did = clean_device_id(payload.get("device_id", ""))
    if not did:
        raise HTTPException(status_code=400, detail="Invalid device_id")

    raw_name = str(payload.get("name") or "Smart Device").strip()[:42] or "Smart Device"
    platform = str(payload.get("platform") or "Web").strip()[:28] or "Web"
    explicit_notify = bool(payload.get("notify", False))

    store = get_devices_store()
    devices = store["devices"]
    now = int(time.time())

    if did in devices:
        dev = devices[did]
        dev["last_seen"] = now
        if raw_name and not dev.get("custom_name"):
            dev["name"] = raw_name
        if platform:
            dev["platform"] = platform
        save_devices_store(store)

        if dev.get("status") == "pending" and explicit_notify:
            asyncio.create_task(notify_owner_device_request(did, dev, is_reaccess=False))

        return {
            "status": dev.get("status", "pending"),
            "device_id": did,
            "name": dev.get("name", raw_name),
            "stopped": SYSTEM_STOPPED,
        }

    pending_cnt = sum(1 for d in devices.values() if d.get("status") == "pending")
    if pending_cnt >= MAX_PENDING_DEVICES:
        raise HTTPException(status_code=429, detail="Too many pending device requests")

    new_dev = {
        "device_id": did,
        "name": raw_name,
        "platform": platform,
        "status": "pending",
        "created_at": now,
        "last_seen": now,
        "movies": [],
        "progress": {},
    }
    devices[did] = new_dev
    save_devices_store(store)
    asyncio.create_task(notify_owner_device_request(did, new_dev, is_reaccess=False))

    return {
        "status": "pending",
        "device_id": did,
        "name": raw_name,
        "stopped": SYSTEM_STOPPED,
    }


@app.post("/api/device/reaccess")
async def api_device_reaccess(payload: dict = Body(...)):
    did = clean_device_id(payload.get("device_id", ""))
    if not did:
        raise HTTPException(status_code=400, detail="Invalid device_id")

    raw_name = str(payload.get("name") or "Smart Device").strip()[:42] or "Smart Device"
    platform = str(payload.get("platform") or "Web").strip()[:28] or "Web"

    store = get_devices_store()
    devices = store["devices"]
    now = int(time.time())
    dev = devices.get(did)

    if not dev:
        dev = {
            "device_id": did,
            "name": raw_name,
            "platform": platform,
            "status": "pending",
            "created_at": now,
            "last_seen": now,
            "movies": [],
            "progress": {},
        }
        devices[did] = dev
    else:
        if dev.get("status") == "approved":
            return {"status": "approved", "device_id": did, "name": dev.get("name", raw_name), "stopped": SYSTEM_STOPPED}
        dev["status"] = "pending"
        dev["last_seen"] = now

    save_devices_store(store)
    approval_notify_ts.pop(did, None)
    asyncio.create_task(notify_owner_device_request(did, dev, is_reaccess=True))
    return {"status": "pending", "device_id": did, "name": dev.get("name", raw_name), "stopped": SYSTEM_STOPPED}


@app.get("/health")
async def health(device_id: str = ""):
    did = clean_device_id(device_id)
    dev_status = "unregistered"
    dev_name = ""
    if did:
        store = get_devices_store()
        dev = store["devices"].get(did)
        if dev:
            dev_status = dev.get("status", "pending")
            dev_name = dev.get("name", "")
            dev["last_seen"] = int(time.time())
            save_devices_store(store)

    return {
        "status": "ok",
        "stopped": SYSTEM_STOPPED,
        "device_status": dev_status,
        "device_name": dev_name,
        "bot_connected": tg_client.is_connected,
        "tunnel_url": get_active_tunnel_url(),
    }


@app.get("/api/events")
async def api_events(request: Request, device_id: str = ""):
    did = clean_device_id(device_id)
    if not did:
        raise HTTPException(status_code=400, detail="device_id required")

    q: asyncio.Queue = asyncio.Queue(maxsize=24)
    device_subscribers.setdefault(did, set()).add(q)
    active_sse_counts[did] = active_sse_counts.get(did, 0) + 1
    touch_device(did)

    async def event_stream():
        try:
            store = get_devices_store()
            dev = store["devices"].get(did)
            init_payload = json.dumps({
                "type": "init",
                "status": dev.get("status", "unregistered") if dev else "unregistered",
                "device_name": dev.get("name", "") if dev else "",
                "stopped": SYSTEM_STOPPED,
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
                    touch_device(did)
                    yield ": keepalive\n\n"
        finally:
            subs = device_subscribers.get(did)
            if subs:
                subs.discard(q)
                if not subs:
                    device_subscribers.pop(did, None)
            cur = max(0, active_sse_counts.get(did, 1) - 1)
            if cur == 0:
                active_sse_counts.pop(did, None)
            else:
                active_sse_counts[did] = cur
            touch_device(did)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Per-Device YPlay Library & Playback Progress ---
@app.get("/api/yplay/library")
async def yplay_library(request: Request, device_id: str = ""):
    did, dev = require_approved_device(request, device_id)
    items = dev.get("movies", [])
    progress_map = dev.get("progress", {})
    enriched = []

    for idx, x in enumerate(items):
        row = dict(x)
        if not row.get("probed") or int(row.get("duration") or 0) <= 0:
            trigger_prewarm(did, row)
        row["index"] = idx
        row["saved_pos"] = int(progress_map.get(row["id"], 0) or 0)
        row["quality"] = detect_quality(
            row.get("title", ""),
            int(row.get("width") or 0),
            int(row.get("height") or 0),
        )
        enriched.append(row)

    return {
        "stopped": SYSTEM_STOPPED,
        "device_name": dev.get("name", ""),
        "count": len(enriched),
        "items": enriched,
    }


@app.post("/api/yplay/progress")
async def yplay_save_progress(request: Request, payload: dict = Body(...)):
    did, _ = require_approved_device(request, str(payload.get("device_id") or ""))
    item_id = str(payload.get("item_id") or "").strip()
    pos = max(0, int(payload.get("position") or 0))
    if not item_id:
        raise HTTPException(status_code=400, detail="item_id required")

    store = get_devices_store()
    dev = store["devices"].get(did)
    if dev:
        prog = dev.setdefault("progress", {})
        if pos > 2:
            prog[item_id] = pos
        else:
            prog.pop(item_id, None)
        save_devices_store(store)
    return {"ok": True}


@app.api_route("/api/yplay/stream/{item_id}", methods=["GET", "HEAD"])
async def yplay_stream(item_id: str, request: Request, device_id: str = ""):
    if SYSTEM_STOPPED and request.query_params.get("internal") != "1":
        raise HTTPException(status_code=503, detail="System shutdown")
    did, _ = require_approved_device(request, device_id)

    item = find_media_item(item_id, did if did != "internal" else "")
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found")

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

    async def continuous_gen():
        pos = start_byte
        while pos <= end_byte:
            if (SYSTEM_STOPPED and request.query_params.get("internal") != "1") or await request.is_disconnected():
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
                    if (SYSTEM_STOPPED and request.query_params.get("internal") != "1") or await request.is_disconnected():
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


@app.api_route("/api/yplay/play/{item_id}", methods=["GET", "HEAD"])
async def yplay_play_stream(item_id: str, request: Request, device_id: str = "", t: float = 0.0, q: str = "orig", a: int = 0):
    global active_play_proc
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")
    did, _ = require_approved_device(request, device_id)

    item = find_media_item(item_id, did)
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found")

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
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{item_id}?internal=1",
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
                if SYSTEM_STOPPED or await request.is_disconnected():
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


@app.get("/api/yplay/sub/{item_id}/{sub_idx}.vtt")
async def yplay_sub_vtt(item_id: str, sub_idx: int, request: Request, device_id: str = "", t: float = 0.0, dur: float = 180.0):
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")
    did, _ = require_approved_device(request, device_id)

    item = find_media_item(item_id, did)
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found")

    start_sec = max(0.0, float(t or 0.0))
    win_sec = min(240.0, max(30.0, float(dur or 180.0)))
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error",
        "-ss", f"{start_sec:.2f}",
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{item_id}?internal=1",
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
async def suntv_m3u8(request: Request, device_id: str = ""):
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")
    require_approved_device(request, device_id)
    ok = await start_suntv_relay()
    m3u8 = HLS_DIR / "index.m3u8"
    if not ok or not m3u8.exists():
        raise HTTPException(status_code=503, detail=last_error or "Sun TV relay not ready")
    return FileResponse(str(m3u8), media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store"})


@app.get("/live/suntv/{seg}")
async def suntv_ts(seg: str, request: Request, device_id: str = ""):
    global last_relay_hit
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")
    require_approved_device(request, device_id)
    last_relay_hit = time.time()
    if "/" in seg or ".." in seg or not seg.endswith(".ts"):
        raise HTTPException(status_code=400, detail="Bad segment")
    p = HLS_DIR / seg
    if not p.exists():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(str(p), media_type="video/mp2t", headers={"Cache-Control": "no-cache"})
