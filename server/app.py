import asyncio
import html
import json
import os
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import urljoin, urlparse

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, StreamingResponse
from pyrogram import Client, enums, filters
from pyrogram.errors import MessageNotModified
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

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = Path("/tmp/yplay_chunks")
LIBRARY_FILE = DATA_DIR / "yplay_library.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
DEVICES_FILE = DATA_DIR / "devices.json"
PROGRESS_FILE = DATA_DIR / "device_progress.json"
OWNER_FILE = DATA_DIR / "owner.json"
STATE_FILE = DATA_DIR / "system_state.json"
SESSION_FILE = DATA_DIR / "ystream_bot"

IST_TZ = timezone(timedelta(hours=5, minutes=30))
CHUNK_SIZE = 1024 * 1024  # 1 MiB
MAX_CACHE_BYTES = int(1.5 * 1024 * 1024 * 1024)  # 1.5 GB cap
DEVICE_ONLINE_TTL = 35  # seconds before device is marked offline
CLIENT_FAIL_COOLDOWN = 600  # 10 min cooldown before re-probing a client-failed channel
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

DEFAULT_CHANNELS = [
    {"num": 1, "id": "suntv", "short": "SUN", "bg": "#d92323", "name": "Sun TV", "type": "hls", "url": "https://offline.ystream.local/suntv/index.m3u8", "logo": "suntv.webp", "status": "offline", "client_failed": False, "failed_at": 0},
    {"num": 2, "id": "vijaytv", "short": "VIJAY", "bg": "#c2410c", "name": "Vijay TV", "type": "hls", "url": "https://da86m1sqpm3o0.cloudfront.net/28072023/smil:starvijayuk.smil/playlist.m3u8", "logo": "vijaytv.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 3, "id": "zeetamil", "short": "ZEE", "bg": "#6d28d9", "name": "Zee Tamil HD", "type": "hls", "url": "https://da86m1sqpm3o0.cloudfront.net/28072023/smil:zeetamil1.smil/playlist.m3u8", "logo": "zeetamil.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 4, "id": "colorstamil", "short": "COLORS", "bg": "#be185d", "name": "Colors Tamil HD", "type": "hls", "url": "https://da86m1sqpm3o0.cloudfront.net/28072023/smil:colorstamilhd11.smil/playlist.m3u8", "logo": "colorstamil.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 5, "id": "vijaytakkar", "short": "TAKKAR", "bg": "#b91c1c", "name": "Vijay Takkar", "type": "dash", "url": "https://tglmp01.akamaized.net/out/v1/c1071012b73f4f189b202e1529e8f802/manifest.mpd", "logo": "vijaytakkar.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 6, "id": "vendhartv", "short": "VENDHAR", "bg": "#0f766e", "name": "Vendhar TV", "type": "hls", "url": "https://mumt04.tangotv.in/m18aqlK4VENDHARTV/index.m3u8", "logo": "vendhartv.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 7, "id": "rajtv", "short": "RAJ", "bg": "#1d4ed8", "name": "Raj TV", "type": "hls", "url": "https://livestream.rajtv.tv/hlslive/Admin/px08241087/live/RAJTV/master_1.m3u8", "logo": "rajtv.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 8, "id": "makkaltv", "short": "MAKKAL", "bg": "#047857", "name": "Makkal TV", "type": "hls", "url": "https://5k8q87azdy4v-hls-live.wmncdn.net/MAKKAL/271ddf829afeece44d8732757fba1a66.sdp/playlist.m3u8", "logo": "makkaltv.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 9, "id": "7smusic", "short": "7S", "bg": "#7e22ce", "name": "7S Music", "type": "hls", "url": "https://cdn.pishow.tv/ott/live/1257/master.m3u8", "logo": "7smusic.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 10, "id": "isaiaruvi", "short": "ISAI", "bg": "#a21caf", "name": "Isaiaruvi", "type": "hls", "url": "https://segment.yuppcdn.net/140622/isaiaruvi/playlist.m3u8", "logo": "isaiaruvi.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 11, "id": "newstamil24", "short": "NEWS24", "bg": "#dc2626", "name": "News Tamil 24x7", "type": "hls", "url": "https://streams.tangotv.in/NEWSTAMIL24X7/ORIGIN/index.m3u8", "logo": "newstamil24.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 12, "id": "puthiyathalaimurai", "short": "PT", "bg": "#b91c1c", "name": "Puthiya Thalaimurai", "type": "hls", "url": "https://mumt07.tangotv.in/zHjX9OFlPUTHIYAEXPRESS/index.m3u8", "logo": "puthiyathalaimurai.webp", "status": "online", "client_failed": False, "failed_at": 0},
    {"num": 13, "id": "discoverytamil", "short": "DISC", "bg": "#0369a1", "name": "Discovery Tamil", "type": "dash-clearkey", "url": "https://login.bheem.tv/bpk-tv/22652/output/index.mpd", "kid": "c6cb0bcbeac25fb286fed851e41de38c", "key": "955c54f2cac8783fafccae5e5f9cdf8d", "logo": "discoverytamil.webp", "status": "online", "client_failed": False, "failed_at": 0},
]

for d in (DATA_DIR, CACHE_DIR):
    d.mkdir(parents=True, exist_ok=True)

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

active_play_proc: Optional[asyncio.subprocess.Process] = None
play_proc_lock = asyncio.Lock()
prewarm_sem = asyncio.Semaphore(1)
msg_cache: dict[str, Message] = {}
prewarm_tasks: dict[str, asyncio.Task] = {}
state_subscribers: set[asyncio.Queue] = set()
bot_dialog_state: dict[int, dict] = {}
bulk_upload_buffers: dict[int, dict] = {}
LAST_SYSTEM_UPDATE: float = time.time()


def read_json(path: Path, default):
    if not path.exists():
        write_json(path, default)
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, data):
    global LAST_SYSTEM_UPDATE
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(tmp, path)
    LAST_SYSTEM_UPDATE = time.time()


def is_valid_device_record(did: str, rec) -> bool:
    if not isinstance(rec, dict):
        return False
    clean_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(rec.get("device_id") or did or ""))
    if len(clean_id) < 6:
        return False
    last_seen = float(rec.get("last_seen") or 0)
    if last_seen <= 0:
        return False
    return True


def init_stores():
    existing = read_json(CHANNELS_FILE, None)
    if not existing or not isinstance(existing, list) or any(not isinstance(c, dict) or "num" not in c for c in existing):
        write_json(CHANNELS_FILE, DEFAULT_CHANNELS)

    devs = read_json(DEVICES_FILE, {})
    if not isinstance(devs, dict):
        write_json(DEVICES_FILE, {})
    else:
        clean_devs = {k: v for k, v in devs.items() if is_valid_device_record(k, v)}
        if len(clean_devs) != len(devs):
            write_json(DEVICES_FILE, clean_devs)

    lib = read_json(LIBRARY_FILE, [])
    if not isinstance(lib, list):
        write_json(LIBRARY_FILE, [])
    else:
        clean_lib = [x for x in lib if isinstance(x, dict)]
        if len(clean_lib) != len(lib):
            write_json(LIBRARY_FILE, clean_lib)

    prog = read_json(PROGRESS_FILE, {})
    if not isinstance(prog, dict):
        write_json(PROGRESS_FILE, {})


init_stores()
_raw_state = read_json(STATE_FILE, {"stopped": False})
SYSTEM_STOPPED: bool = bool(_raw_state.get("stopped", False)) if isinstance(_raw_state, dict) else False


def broadcast_event(event_type: str = "state", extra: Optional[dict] = None):
    data = {
        "type": event_type,
        "stopped": SYSTEM_STOPPED,
        "ts": time.time(),
    }
    if extra:
        data.update(extra)
    payload = json.dumps(data)
    for q in list(state_subscribers):
        try:
            q.put_nowait(payload)
        except Exception:
            pass


def set_system_stopped(stopped: bool):
    global SYSTEM_STOPPED
    SYSTEM_STOPPED = bool(stopped)
    write_json(STATE_FILE, {"stopped": SYSTEM_STOPPED, "updated_at": int(time.time())})
    broadcast_event("state")


def get_owner_id() -> int:
    if ENV_OWNER_ID > 0:
        return ENV_OWNER_ID
    data = read_json(OWNER_FILE, {"owner_id": 0})
    if not isinstance(data, dict):
        return 0
    try:
        return int(data.get("owner_id") or 0)
    except Exception:
        return 0


def authorize_user(user: Optional[User], allow_claim: bool = True) -> bool:
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


async def safe_edit_message(msg: Message, text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    try:
        await msg.edit_text(
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except MessageNotModified:
        pass
    except Exception as exc:
        print(f"[Edit message error] {exc}")


async def notify_owner(text: str, reply_markup: Optional[InlineKeyboardMarkup] = None):
    owner = get_owner_id()
    if owner <= 0 or not tg_client.is_connected:
        return
    try:
        await tg_client.send_message(
            owner,
            text,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=reply_markup,
        )
    except Exception as exc:
        print(f"[Notify owner error] {exc}")


def get_active_tunnel_url() -> str:
    for cmd in (
        ["journalctl", "-u", "ystream-tunnel", "-n", "80", "--no-pager"],
        ["sudo", "-n", "journalctl", "-u", "ystream-tunnel", "-n", "80", "--no-pager"],
    ):
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL, timeout=3)
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


def fmt_ago(ts: float) -> str:
    if not ts or ts <= 0:
        return "Never"
    diff = int(max(0, time.time() - ts))
    if diff < 10:
        return "Just now"
    if diff < 60:
        return f"{diff}s ago"
    if diff < 3600:
        return f"{diff // 60}m ago"
    if diff < 86400:
        return f"{diff // 3600}h ago"
    return datetime.fromtimestamp(ts, tz=IST_TZ).strftime("%d %b %I:%M %p")


def fmt_ist_full(ts: float) -> str:
    if not ts or ts <= 0:
        return "Unknown"
    return datetime.fromtimestamp(ts, tz=IST_TZ).strftime("%d %b %Y, %I:%M:%S %p IST")


def fmt_clock_now() -> str:
    return datetime.now(IST_TZ).strftime("%I:%M:%S %p IST")


def truncate_text(text: str, max_len: int) -> str:
    s = re.sub(r"\s+", " ", str(text or "")).strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3].rstrip() + "..."


# =============================================================================
# DEVICE DETECTION & LIVE ACTIVITY TRACKING
# =============================================================================
def classify_device_type(device_id: str, user_agent: str, native_model: str = "") -> str:
    did = str(device_id or "").lower()
    ua = str(user_agent or "").lower()
    if did.startswith("tv_") or native_model or any(x in ua for x in ("smart-tv", "smarttv", "googletv", "android tv", "tizen", "webos", "bravia", "mitv", "aft")):
        return "Android TV / Smart TV"
    if did.startswith("mob_") or any(x in ua for x in ("mobile", "iphone", "ipad", "android")):
        return "Mobile Phone / Tablet"
    return "Desktop / Web Browser"


def detect_device_name(
    user_agent: str,
    client_model: str = "",
    platform: str = "",
    device_id: str = "",
) -> str:
    cm = re.sub(r"\s+", " ", str(client_model or "")).strip()
    if cm and cm.upper() != "K" and cm.lower() not in ("unknown", "android", "generic", "sdk", "device"):
        low_cm = cm.lower()
        if "mitv" in low_cm or "mibox" in low_cm:
            return truncate_text(f"Xiaomi TV ({cm})", 28)
        if re.match(r"^(21|22|23|24|25)\d{4,}", cm) or "redmi" in low_cm or "poco" in low_cm:
            return truncate_text(f"Xiaomi/Redmi ({cm})", 28)
        if cm.upper().startswith("SM-"):
            return truncate_text(f"Samsung ({cm})", 28)
        if cm.upper().startswith(("RMX", "CPH", "V2", "LE2", "NE2")):
            return truncate_text(f"Android ({cm})", 28)
        return truncate_text(cm, 28)

    ua = str(user_agent or "")
    m = re.search(r";\s*([^;()]+?)\s+Build/", ua)
    if m:
        raw_model = m.group(1).strip()
        if raw_model.upper() != "K" and raw_model.lower() not in ("android", "unknown"):
            low_m = raw_model.lower()
            if "mitv" in low_m or "mibox" in low_m or "xiaomi" in low_m:
                return truncate_text(f"Xiaomi TV ({raw_model})", 28)
            if "bravia" in low_m:
                return truncate_text(f"Sony TV ({raw_model})", 28)
            if "aft" in low_m:
                return truncate_text(f"Fire TV ({raw_model})", 28)
            if "shield" in low_m:
                return "NVIDIA Shield TV"
            return truncate_text(raw_model, 28)

    low_ua = ua.lower()
    browser = ""
    if "edg/" in low_ua:
        browser = "Edge"
    elif "opr/" in low_ua or "opera" in low_ua:
        browser = "Opera"
    elif "firefox/" in low_ua:
        browser = "Firefox"
    elif "chrome/" in low_ua or "crios/" in low_ua:
        browser = "Chrome"
    elif "safari/" in low_ua:
        browser = "Safari"

    if str(device_id).startswith("tv_"):
        return "Android TV App"
    if "tizen" in low_ua or "samsung" in low_ua:
        return "Samsung Smart TV"
    if "webos" in low_ua or "lg netcast" in low_ua:
        return "LG webOS TV"
    if "android tv" in low_ua or "googletv" in low_ua or "aft" in low_ua:
        return "Android TV"
    if "iphone" in low_ua:
        return f"Apple iPhone ({browser})" if browser else "Apple iPhone"
    if "ipad" in low_ua:
        return f"Apple iPad ({browser})" if browser else "Apple iPad"
    if "android" in low_ua:
        return f"Android Phone ({browser})" if browser else "Android Phone"
    if "windows" in low_ua:
        return f"Windows PC ({browser})" if browser else "Windows PC"
    if "macintosh" in low_ua or "mac os" in low_ua:
        return f"MacBook / Mac ({browser})" if browser else "MacBook / Mac"
    if "linux" in low_ua:
        return f"Linux PC ({browser})" if browser else "Linux Browser"
    plat = str(platform or "").strip()
    if plat:
        return truncate_text(f"{plat} ({browser})" if browser else plat, 28)
    return "Smart TV / Web"


def record_device_activity(
    device_id: str,
    user_agent: str = "",
    client_model: str = "",
    platform: str = "",
    activity: Optional[str] = None,
    playing: Optional[str] = None,
) -> Optional[dict]:
    clean_id = re.sub(r"[^a-zA-Z0-9_-]", "", str(device_id or ""))[:64]
    if len(clean_id) < 6:
        return None

    raw_devs = read_json(DEVICES_FILE, {})
    if not isinstance(raw_devs, dict):
        raw_devs = {}
    raw_devs = {k: v for k, v in raw_devs.items() if is_valid_device_record(k, v)}

    now = time.time()
    rec = raw_devs.get(clean_id)
    ua_clean = str(user_agent or "").strip()
    model_clean = str(client_model or "").strip()
    plat_clean = str(platform or "").strip()

    if not isinstance(rec, dict):
        detected_name = detect_device_name(ua_clean, model_clean, plat_clean, clean_id)
        rec = {
            "device_id": clean_id,
            "name": detected_name,
            "device_type": classify_device_type(clean_id, ua_clean, model_clean),
            "user_agent": ua_clean[:240],
            "client_model": model_clean[:60],
            "created_at": int(now),
            "last_seen": now,
            "activity": activity if activity in ("live", "yplay", "idle") else "idle",
            "playing": truncate_text(playing, 64) if playing else "",
        }
    else:
        if ua_clean:
            rec["user_agent"] = ua_clean[:240]
        if model_clean and model_clean.upper() != "K":
            rec["client_model"] = model_clean[:60]

        cur_name = str(rec.get("name") or "")
        generic_names = ("Device", "K", "Smart TV / Web", "Android Phone", "Android Phone (Chrome)", "Android TV", "Windows PC", "Windows PC (Chrome)")
        if (model_clean and model_clean.upper() != "K") or cur_name in generic_names or not cur_name:
            better = detect_device_name(
                rec.get("user_agent", ua_clean),
                rec.get("client_model", model_clean),
                plat_clean,
                clean_id,
            )
            if better:
                rec["name"] = better

        if not rec.get("device_type"):
            rec["device_type"] = classify_device_type(clean_id, rec.get("user_agent", ua_clean), rec.get("client_model", model_clean))

        rec["last_seen"] = now
        if activity in ("live", "yplay", "idle"):
            rec["activity"] = activity
        if playing is not None and str(playing).strip():
            rec["playing"] = truncate_text(str(playing), 64)

    raw_devs[clean_id] = rec
    write_json(DEVICES_FILE, raw_devs)
    return rec


def get_devices_ordered() -> list[dict]:
    raw = read_json(DEVICES_FILE, {})
    if not isinstance(raw, dict):
        write_json(DEVICES_FILE, {})
        return []

    clean_map = {k: v for k, v in raw.items() if is_valid_device_record(k, v)}
    if len(clean_map) != len(raw):
        write_json(DEVICES_FILE, clean_map)

    items = [dict(v) for v in clean_map.values()]
    items.sort(key=lambda d: int(d.get("created_at") or 0))
    now = time.time()
    for idx, d in enumerate(items, start=1):
        d["num"] = idx
        if not d.get("name") or str(d.get("name")).strip() in ("Device", "K"):
            d["name"] = detect_device_name(
                d.get("user_agent", ""),
                d.get("client_model", ""),
                "",
                d.get("device_id", ""),
            )
        last_seen = float(d.get("last_seen") or 0)
        d["is_online"] = (now - last_seen) <= DEVICE_ONLINE_TTL
    return items


def count_online_devices() -> int:
    return sum(1 for d in get_devices_ordered() if d.get("is_online"))


def build_device_dashboard_text() -> tuple[str, InlineKeyboardMarkup]:
    devices = get_devices_ordered()
    total = len(devices)
    active_cnt = sum(1 for d in devices if d.get("is_online"))

    lines = [
        "<b>📱 Device Management</b>",
        f"Total Devices: <code>{total}</code>  |  Live Now: <code>{active_cnt}</code>",
        f"Updated: <code>{fmt_clock_now()}</code>\n",
    ]

    if not devices:
        lines.append("<i>No devices have opened the app or website yet.</i>")
        return "\n".join(lines), InlineKeyboardMarkup([
            [InlineKeyboardButton("🔄 Refresh", callback_data="dev:refresh")]
        ])

    table_rows = ["No.  Device Name            Activity", "------------------------------------------"]
    for d in devices:
        num_str = str(d.get("num", 0)).ljust(4)
        name_str = truncate_text(d.get("name") or "Smart TV / Web", 21).ljust(22)
        playing = truncate_text(d.get("playing") or "", 30)
        if not d.get("is_online"):
            last_str = fmt_ago(float(d.get("last_seen") or 0))
            act_line = f"Offline ({last_str})"
            sub_line = f"     Last Watched: {playing}" if playing else ""
        else:
            act = d.get("activity") or "idle"
            if act == "live":
                act_line = "📺 Live TV"
                sub_line = f"     Currently Playing: {playing}" if playing else ""
            elif act == "yplay":
                act_line = "🎬 YPlay"
                sub_line = f"     Currently Playing: {playing}" if playing else ""
            else:
                act_line = "🟢 Online"
                sub_line = ""
        table_rows.append(f"{num_str} {name_str} {act_line}")
        if sub_line:
            table_rows.append(sub_line)

    lines.append(f"<pre>{html.escape(chr(10).join(table_rows))}</pre>")
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("ℹ️ Details", callback_data="dev:info_prompt"),
            InlineKeyboardButton("🗑 Remove", callback_data="dev:remove_prompt"),
        ],
        [InlineKeyboardButton("🔄 Refresh", callback_data="dev:refresh")],
    ])
    return "\n".join(lines), kb


# =============================================================================
# /YStream — LIVE TV CHANNEL MANAGEMENT & STABLE RECOVERY MONITOR
# =============================================================================
def get_channels_sorted() -> list[dict]:
    chans = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
    if not isinstance(chans, list):
        return list(DEFAULT_CHANNELS)
    valid = [c for c in chans if isinstance(c, dict)]
    valid.sort(key=lambda c: int(c.get("num") or 0))
    return valid


def fetch_url_text_sync(url: str, timeout: int = 5) -> tuple[int, str]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/131.0.0.0 Safari/537.36",
            "Accept": "*/*",
        },
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        code = getattr(resp, "status", 200)
        body = resp.read(4096).decode("utf-8", errors="ignore")
        return code, body


def probe_stream_url_sync(url: str) -> str:
    if not url or "offline.ystream.local" in url:
        return "offline"
    try:
        code, sample = fetch_url_text_sync(url, timeout=5)
        if code < 200 or code >= 400:
            return "offline"
        low_u = url.lower()
        if ".m3u8" in low_u:
            if "#EXTM3U" not in sample:
                return "offline"
            for line in sample.splitlines():
                ln = line.strip()
                if ln and not ln.startswith("#"):
                    child_url = urljoin(url, ln)
                    if ".m3u8" in child_url.lower():
                        c_code, c_body = fetch_url_text_sync(child_url, timeout=5)
                        if c_code < 200 or c_code >= 400 or "#EXTM3U" not in c_body:
                            return "offline"
                    break
            return "online"
        if ".mpd" in low_u:
            return "online" if ("<MPD" in sample or "urn:mpeg:dash" in sample) else "offline"
        return "online" if len(sample) > 0 else "offline"
    except urllib.error.HTTPError as e:
        if e.code == 403:
            return "geo_blocked"
        return "offline"
    except Exception:
        return "offline"


async def live_check_channels(manual_refresh: bool = False):
    chans = get_channels_sorted()
    if not chans:
        return

    now = time.time()
    targets = []
    for c in chans:
        if c.get("status") == "online":
            continue
        if not manual_refresh and c.get("client_failed"):
            failed_at = float(c.get("failed_at") or 0)
            if (now - failed_at) < CLIENT_FAIL_COOLDOWN:
                continue
        targets.append(c)

    if not targets:
        return

    results = await asyncio.gather(
        *(asyncio.to_thread(probe_stream_url_sync, c.get("url", "")) for c in targets),
        return_exceptions=True,
    )

    changed = False
    recovered_list = []
    for ch, res in zip(targets, results):
        if isinstance(res, Exception):
            res = "offline"
        if res == "online":
            ch["status"] = "online"
            ch["client_failed"] = False
            ch["failed_at"] = 0
            changed = True
            recovered_list.append(ch)

    if changed:
        write_json(CHANNELS_FILE, chans)
        broadcast_event("channels")

    for ch in recovered_list:
        table = (
            "No.  Channel Name             Status\n"
            "--------------------------------------\n"
            f"{str(ch['num']).ljust(4)} {truncate_text(ch['name'], 23).ljust(24)} 🟢"
        )
        await notify_owner(f"✅ <b>Channel Recovered & Restored</b>\n<pre>{html.escape(table)}</pre>")


def build_ystream_dashboard() -> tuple[str, InlineKeyboardMarkup]:
    chans = get_channels_sorted()
    rows = ["No.  Channel Name             Status", "--------------------------------------"]
    for c in chans:
        n_str = str(c.get("num")).ljust(4)
        name_str = truncate_text(c.get("name") or "Channel", 23).ljust(24)
        st = "🟢" if c.get("status") == "online" else "🔴"
        rows.append(f"{n_str} {name_str} {st}")
    if not chans:
        rows.append("No channels configured.")

    txt = (
        f"<b>📺 YStream — Live TV Channels</b>\n"
        f"Updated: <code>{fmt_clock_now()}</code>\n"
        f"<pre>{html.escape(chr(10).join(rows))}</pre>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add", callback_data="ys:add"),
            InlineKeyboardButton("🗑 Remove", callback_data="ys:remove"),
            InlineKeyboardButton("✏️ Edit", callback_data="ys:edit"),
        ],
        [InlineKeyboardButton("🔄 Refresh", callback_data="ys:refresh")],
    ])
    return txt, kb


def validate_channel_lines(raw_text: str) -> tuple[bool, str, Optional[tuple[str, str, str]]]:
    lines = [ln.strip() for ln in (raw_text or "").strip().splitlines() if ln.strip()]
    if len(lines) != 3:
        return False, (
            "❌ <b>Invalid Format!</b>\nPlease send exact 3 lines:\n"
            "<code>Channel Name\nLive Stream Link\nlogoname.webp</code>"
        ), None

    name, url, logo = lines
    if len(name) < 2 or len(name) > 40:
        return False, "❌ <b>Invalid Channel Name:</b> Must be between 2 and 40 characters.", None

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc or " " in url:
        return False, "❌ <b>Invalid Stream Link:</b> Must be a valid <code>http://</code> or <code>https://</code> URL.", None

    logo_clean = logo.replace("assets/", "").strip()
    if not re.match(r"^[a-zA-Z0-9._-]+\.webp$", logo_clean, re.IGNORECASE):
        return False, "❌ <b>Invalid Logo Name:</b> Must end with <code>.webp</code> (example: <code>suntv.webp</code>).", None

    return True, "", (name, url, logo_clean)


def make_channel_obj(num: int, name: str, url: str, logo: str) -> dict:
    palette = ["#d92323", "#c2410c", "#6d28d9", "#be185d", "#0f766e", "#1d4ed8", "#047857", "#7e22ce", "#0369a1"]
    short = re.sub(r"[^A-Za-z0-9]", "", name).upper()[:6] or f"CH{num}"
    cid = f"ch_{num}_{re.sub(r'[^a-z0-9]', '', name.lower())[:12]}"
    stype = "dash" if ".mpd" in url.lower() else "hls"
    return {
        "num": int(num),
        "id": cid,
        "short": short,
        "bg": palette[num % len(palette)],
        "name": name,
        "type": stype,
        "url": url,
        "logo": logo,
        "status": "online",
        "client_failed": False,
        "failed_at": 0,
    }


async def mark_channel_status(num: int, new_status: str, notify: bool = True, from_client: bool = False):
    chans = get_channels_sorted()
    target = next((c for c in chans if int(c.get("num") or 0) == int(num)), None)
    if not target:
        return
    old_status = target.get("status", "online")

    if new_status == "offline":
        target["status"] = "offline"
        if from_client:
            target["client_failed"] = True
            target["failed_at"] = int(time.time())
        write_json(CHANNELS_FILE, chans)
        broadcast_event("channels")
        if notify and old_status != "offline":
            table = (
                "No.  Channel Name             Status\n"
                "--------------------------------------\n"
                f"{str(target['num']).ljust(4)} {truncate_text(target['name'], 23).ljust(24)} 🔴"
            )
            await notify_owner(f"⚠️ <b>Channel Stream Offline</b>\n<pre>{html.escape(table)}</pre>")
    else:
        target["status"] = "online"
        target["client_failed"] = False
        target["failed_at"] = 0
        write_json(CHANNELS_FILE, chans)
        broadcast_event("channels")
        if notify and old_status != "online":
            table = (
                "No.  Channel Name             Status\n"
                "--------------------------------------\n"
                f"{str(target['num']).ljust(4)} {truncate_text(target['name'], 23).ljust(24)} 🟢"
            )
            await notify_owner(f"✅ <b>Channel Recovered & Restored</b>\n<pre>{html.escape(table)}</pre>")


async def zero_idle_channel_recovery_loop():
    """Runs zero checks when 0 devices are online OR 0 channels are offline."""
    while True:
        await asyncio.sleep(30)
        try:
            if SYSTEM_STOPPED:
                continue
            if count_online_devices() == 0:
                continue
            await live_check_channels(manual_refresh=False)
        except Exception as exc:
            print(f"[Recovery monitor error] {exc}")


# =============================================================================
# /YPlay — MEDIA QUALITY, PROBING, CACHE & PER-DEVICE PROGRESS
# =============================================================================
def get_library_items() -> list[dict]:
    items = read_json(LIBRARY_FILE, [])
    if not isinstance(items, list):
        return []
    return [x for x in items if isinstance(x, dict)]


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
    size = int(getattr(media, "file_size", None) or 0)
    raw_dur = int(getattr(media, "duration", None) or 0)
    duration = 0 if (size > 20 * 1024 * 1024 and raw_dur < 30) else raw_dur
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
        "file_size": size,
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
        "status": "working",
        "added_at": int(time.time()),
    }


def format_yplay_table_rows(items: list[dict], start_num: int = 1) -> str:
    lines = ["No.  File Title", "----------------------------------------------"]
    for offset, m in enumerate(items):
        num_str = str(start_num + offset).ljust(4)
        title_str = truncate_text(m.get("title") or "Untitled", 34)
        if m.get("status") == "error":
            lines.append(f"{num_str} {title_str} 🔴")
        else:
            q_str = str(m.get("quality") or "HD").upper()
            tracks = m.get("audio_tracks") or []
            lang_cnt = max(1, len({str(t.get("label") or "").lower() for t in tracks if isinstance(t, dict) and t.get("label")})) if tracks else 1
            lines.append(f"{num_str} {title_str}")
            lines.append(f"     {q_str} · {lang_cnt}Lang · 🟢")
    if not items:
        lines.append("No files in YPlay.")
    return "\n".join(lines)


def build_yplay_dashboard() -> tuple[str, InlineKeyboardMarkup]:
    items = get_library_items()
    for row in items:
        if not row.get("probed"):
            trigger_prewarm(row)
    table = format_yplay_table_rows(items, start_num=1)
    txt = (
        f"<b>🎬 YPlay — Movie & File Library</b>\n"
        f"Updated: <code>{fmt_clock_now()}</code>\n"
        f"<pre>{html.escape(table)}</pre>"
    )
    kb = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("➕ Add", callback_data="yp:add"),
            InlineKeyboardButton("🗑 Remove", callback_data="yp:remove"),
        ],
        [InlineKeyboardButton("🔄 Refresh", callback_data="yp:refresh")],
    ])
    return txt, kb


def safe_item_dir(item_id: str) -> Path:
    safe = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
    p = CACHE_DIR / safe
    p.mkdir(parents=True, exist_ok=True)
    return p


def block_path(item_id: str, b_idx: int) -> Path:
    return safe_item_dir(item_id) / f"{b_idx}.blk"


def delete_item_cache_sync(item_id: str):
    safe = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
    p = CACHE_DIR / safe
    if p.exists() and p.is_dir():
        shutil.rmtree(p, ignore_errors=True)


def remove_item_from_all_device_progress(item_ids: list[str]):
    if not item_ids:
        return
    prog = read_json(PROGRESS_FILE, {})
    if not isinstance(prog, dict):
        return
    changed = False
    for did in list(prog.keys()):
        if isinstance(prog[did], dict):
            for iid in item_ids:
                if iid in prog[did]:
                    prog[did].pop(iid, None)
                    changed = True
    if changed:
        write_json(PROGRESS_FILE, prog)


def write_block_sync(path: Path, data: bytes):
    if path.exists():
        return
    tmp = path.with_suffix(f".{os.getpid()}.tmp")
    tmp.write_bytes(data)
    os.replace(tmp, path)


def cleanup_orphan_tmps_sync():
    for tmp in CACHE_DIR.glob("*/*.tmp"):
        try:
            tmp.unlink()
        except Exception:
            pass


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


def run_ffprobe_http(item_id: str) -> Optional[dict]:
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-analyzeduration", "10000000",
            "-probesize", "10000000",
            "-show_entries", "format=duration:stream=codec_type,codec_name,width,height:stream_tags=language,title",
            "-of", "json",
            f"http://127.0.0.1:8000/api/yplay/stream/{item_id}",
        ]
        res = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL, timeout=14)
        return json.loads(res)
    except Exception:
        return None


def parse_ffprobe_data(data: dict, file_size: int = 0) -> Optional[dict]:
    if not isinstance(data, dict):
        return None
    streams = data.get("streams") or []
    if not streams or not isinstance(streams, list):
        return None
    v_stream = next((s for s in streams if isinstance(s, dict) and s.get("codec_type") == "video"), {})
    fmt_info = data.get("format") if isinstance(data.get("format"), dict) else {}

    audio_tracks = []
    a_idx = 0
    for s in streams:
        if not isinstance(s, dict) or s.get("codec_type") != "audio":
            continue
        tags = s.get("tags") if isinstance(s.get("tags"), dict) else {}
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
        if not isinstance(s, dict) or s.get("codec_type") != "subtitle":
            continue
        codec = str(s.get("codec_name") or "").lower()
        if codec in TEXT_SUB_CODECS:
            tags = s.get("tags") if isinstance(s.get("tags"), dict) else {}
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

    if file_size > 20 * 1024 * 1024 and dur < 30:
        dur = 0

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
        "status": "working" if (w > 0 or audio_tracks) else "error",
    }


def probe_media_sync(item_id: str, file_size: int = 0) -> Optional[dict]:
    raw = run_ffprobe_http(item_id)
    if raw:
        parsed = parse_ffprobe_data(raw, file_size)
        if parsed and (parsed["width"] > 0 or parsed["audio_tracks"]):
            return parsed
    return None


def update_library_probe(item_id: str, info: dict):
    items = get_library_items()
    changed = False
    for row in items:
        if row.get("id") == item_id:
            row["probed"] = True
            row["status"] = info.get("status", "working")
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
            if info.get("duration") and int(info["duration"]) > int(row.get("duration") or 0):
                row["duration"] = int(info["duration"])
            row["quality"] = detect_quality(
                row.get("title", ""),
                int(row.get("width") or 0),
                int(row.get("height") or 0),
            )
            changed = True
            break
    if changed:
        write_json(LIBRARY_FILE, items)
        broadcast_event("library")


def find_yplay_item(item_ref: str) -> tuple[Optional[int], Optional[dict]]:
    items = get_library_items()
    for i, row in enumerate(items):
        if str(row.get("id")) == str(item_ref):
            return i, row
    if str(item_ref).isdigit():
        idx = int(item_ref)
        if 0 <= idx < len(items):
            return idx, items[idx]
    return None, None


async def kill_active_play_proc():
    global active_play_proc
    async with play_proc_lock:
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
        async with prewarm_sem:
            msg = await get_tg_message(item)
            media = msg.video or msg.document or msg.animation
            total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
            if total_size <= 0:
                return
            total_blocks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE

            head_count = min(2, total_blocks)
            if not all(block_path(item_id, b).exists() for b in range(head_count)):
                await cache_span(item, 0, head_count)

            if total_blocks > 3:
                tail_start = max(head_count, total_blocks - 2)
                tail_count = total_blocks - tail_start
                if not all(block_path(item_id, b).exists() for b in range(tail_start, total_blocks)):
                    await cache_span(item, tail_start, tail_count)

            probe_res = await asyncio.to_thread(probe_media_sync, item_id, total_size)
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
    if block_path(item_id, 0).exists() and item.get("probed") and int(item.get("duration") or 0) >= 30:
        return
    prewarm_tasks[item_id] = asyncio.create_task(prewarm_head_tail(item))


def save_media_to_queue(item: dict) -> tuple[bool, int, dict]:
    items = get_library_items()
    for idx, row in enumerate(items, start=1):
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            trigger_prewarm(row)
            broadcast_event("library")
            return False, idx, row
    items.append(item)
    write_json(LIBRARY_FILE, items)
    broadcast_event("library")
    trigger_prewarm(item)
    return True, len(items), item


# =============================================================================
# /status — INSTANT REAL-TIME SYSTEM STATUS
# =============================================================================
async def build_status_html() -> str:
    devices = get_devices_ordered()
    dev_total = len(devices)
    dev_online = sum(1 for d in devices if d.get("is_online"))
    dev_offline = dev_total - dev_online
    dev_live = sum(1 for d in devices if d.get("is_online") and d.get("activity") == "live")
    dev_yplay = sum(1 for d in devices if d.get("is_online") and d.get("activity") == "yplay")

    chans = get_channels_sorted()
    ch_total = len(chans)
    ch_working = sum(1 for c in chans if c.get("status") == "online")
    ch_offline = ch_total - ch_working
    ch_monitored = ch_offline if (dev_online > 0 and not SYSTEM_STOPPED) else 0

    yp_items = get_library_items()
    yp_total = len(yp_items)
    yp_working = sum(1 for m in yp_items if m.get("status") != "error")
    yp_error = yp_total - yp_working
    yp_playing = dev_yplay

    blk_cnt, blk_bytes = await asyncio.to_thread(get_cache_stats_sync)
    sys = await asyncio.to_thread(get_sys_stats)
    tunnel = await asyncio.to_thread(get_active_tunnel_url)

    monitor_status = (
        "Suspended (Shutdown)" if SYSTEM_STOPPED
        else (f"Active ({ch_monitored} offline)" if ch_monitored > 0 else "Idle (Zero-Load)")
    )

    sections = [
        "<b>📊 YStream Complete System Status</b>",
        f"Updated: <code>{fmt_clock_now()}</code>\n",
        "<b>A. Device Statistics</b>",
        "<pre>" + html.escape("\n".join([
            f"Total Devices         : {dev_total}",
            f"Currently Live (🟢)   : {dev_online}",
            f"Currently Offline     : {dev_offline}",
            f"Watching Live TV (📺) : {dev_live}",
            f"Watching YPlay (🎬)   : {dev_yplay}",
        ])) + "</pre>\n",
        "<b>B. Live TV Statistics</b>",
        "<pre>" + html.escape("\n".join([
            f"Configured Channels   : {ch_total}",
            f"Working Channels (🟢) : {ch_working}",
            f"Offline Channels (🔴) : {ch_offline}",
            f"Actively Monitored    : {ch_monitored}",
        ])) + "</pre>\n",
        "<b>C. YPlay Statistics</b>",
        "<pre>" + html.escape("\n".join([
            f"Total Files Available : {yp_total}",
            f"Playable Files (🟢)   : {yp_working}",
            f"Error Files (🔴)      : {yp_error}",
            f"Currently Playing     : {yp_playing}",
        ])) + "</pre>\n",
        "<b>D. System Activity</b>",
        "<pre>" + html.escape("\n".join([
            f"Telegram Bot          : {'Connected' if tg_client.is_connected else 'Offline'}",
            f"Server Status         : {'SYSTEM SHUTDOWN' if SYSTEM_STOPPED else 'ACTIVE'}",
            f"Live TV Service       : {'Stopped' if SYSTEM_STOPPED else 'Operational'}",
            f"YPlay Service         : {'Stopped' if SYSTEM_STOPPED else 'Operational'}",
            f"Background Monitor    : {monitor_status}",
            f"Active Sessions       : {dev_live + dev_yplay}",
            f"Last System Update    : {fmt_ago(LAST_SYSTEM_UPDATE)}",
            f"Cache / RAM / Disk    : {fmt_bytes(blk_bytes)} | {sys['mem_used_mb']}/{sys['mem_total_mb']}MB | {sys['disk_used_gb']:.1f}/{sys['disk_total_gb']:.1f}GB",
            f"Server Uptime         : {fmt_uptime(time.time() - START_TIME)}",
        ])) + "</pre>",
    ]

    if tunnel:
        sections.append(f"\n<b>Tunnel URL:</b>\n<code>{html.escape(tunnel)}</code>")
    return "\n".join(sections)


# =============================================================================
# TELEGRAM BOT COMMANDS & INTERACTIVE MANAGEMENT
# =============================================================================
@tg_client.on_message(filters.private & filters.command("start"))
async def cmd_start(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        state_txt = "SYSTEM SHUTDOWN (send /resume)" if SYSTEM_STOPPED else "ACTIVE"
        await msg.reply_text(
            f"<b>YStream Bot Active</b>\n"
            f"System State: <code>{state_txt}</code>\n\n"
            f"• /YStream — Manage Live TV channels\n"
            f"• /YPlay — Manage YPlay movies & files\n"
            f"• /device — View live & past devices\n"
            f"• /status — Complete system status",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        print(f"[/start error] {exc}")


@tg_client.on_message(filters.private & filters.command("ystream"))
async def cmd_ystream(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        txt, kb = build_ystream_dashboard()
        await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception as exc:
        await msg.reply_text(f"⚠️ Error loading /YStream: <code>{html.escape(str(exc))}</code>", parse_mode=enums.ParseMode.HTML)


@tg_client.on_message(filters.private & filters.command("yplay"))
async def cmd_yplay(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        txt, kb = build_yplay_dashboard()
        await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception as exc:
        await msg.reply_text(f"⚠️ Error loading /YPlay: <code>{html.escape(str(exc))}</code>", parse_mode=enums.ParseMode.HTML)


@tg_client.on_message(filters.private & filters.command("device"))
async def cmd_device(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        txt, kb = build_device_dashboard_text()
        await msg.reply_text(txt, parse_mode=enums.ParseMode.HTML, reply_markup=kb)
    except Exception as exc:
        await msg.reply_text(f"⚠️ Error loading /device: <code>{html.escape(str(exc))}</code>", parse_mode=enums.ParseMode.HTML)


@tg_client.on_message(filters.private & filters.command("status"))
async def cmd_status(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        txt = await build_status_html()
        await msg.reply_text(
            txt,
            parse_mode=enums.ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="st:refresh")]]),
        )
    except Exception as exc:
        await msg.reply_text(f"⚠️ Error loading /status: <code>{html.escape(str(exc))}</code>", parse_mode=enums.ParseMode.HTML)


@tg_client.on_message(filters.private & filters.command("shutdown"))
async def cmd_shutdown(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        set_system_stopped(True)
        await kill_active_play_proc()
        await msg.reply_text(
            "<b>System Shutdown Activated.</b>\nAll TV & Browser screens are now blacked out with the System Shutdown alert.\nSend /resume to continue playback immediately.",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        print(f"[/shutdown error] {exc}")


@tg_client.on_message(filters.private & filters.command(["resume", "poweron"]))
async def cmd_resume(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        bot_dialog_state.pop(int(msg.from_user.id), None)
        set_system_stopped(False)
        await msg.reply_text(
            "<b>System Resumed.</b>\nPlayback restored immediately on TV & Browser.",
            parse_mode=enums.ParseMode.HTML,
        )
    except Exception as exc:
        print(f"[/resume error] {exc}")


async def flush_bulk_yplay_upload(uid: int):
    await asyncio.sleep(1.2)
    bundle = bulk_upload_buffers.pop(uid, None)
    if not bundle:
        return
    added_records = bundle.get("added") or []
    chat_id = bundle.get("chat_id")
    if not added_records or not chat_id:
        return

    if len(added_records) == 1:
        pos, item = added_records[0]
        table = format_yplay_table_rows([item], start_num=pos)
        await tg_client.send_message(
            chat_id,
            f"<pre>{html.escape(table)}</pre>\n\n✅ File successfully added at position <code>{pos}</code> in YPlay.",
            parse_mode=enums.ParseMode.HTML,
        )
    else:
        lines = ["No.  File Title", "----------------------------------------------"]
        for pos, item in added_records:
            q_str = str(item.get("quality") or "HD").upper()
            lines.append(f"{str(pos).ljust(4)} {truncate_text(item.get('title') or 'Untitled', 34)}")
            lines.append(f"     {q_str} · 1Lang · 🟢")
        await tg_client.send_message(
            chat_id,
            f"✅ <b>{len(added_records)} files successfully added to YPlay:</b>\n<pre>{html.escape(chr(10).join(lines))}</pre>",
            parse_mode=enums.ParseMode.HTML,
        )


@tg_client.on_message(
    filters.private
    & ~filters.command(["start", "ystream", "yplay", "device", "status", "shutdown", "resume", "poweron"])
)
async def on_interactive_input(_, msg: Message):
    try:
        if not authorize_user(msg.from_user, allow_claim=True):
            return
        uid = int(msg.from_user.id)
        st = bot_dialog_state.get(uid)
        mode = st.get("mode") if isinstance(st, dict) else None

        has_any_media = bool(
            msg.video or msg.document or msg.animation or msg.photo or msg.audio or msg.voice or msg.sticker
        )

        if has_any_media:
            if mode != "yp_add":
                await msg.reply_text(
                    "⚠️ <b>Upload Blocked</b>\nFiles shared directly are not added to YPlay.\nPlease open /YPlay and tap <b>➕ Add</b> first.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            if not is_video_msg(msg):
                await msg.reply_text(
                    "❌ <b>Unsupported File Format</b>\nOnly video files (<code>.mp4</code>, <code>.mkv</code>, <code>.webm</code>, <code>.mov</code>, <code>.avi</code>) are accepted. PDFs, images and other formats are rejected.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            info = get_media_info(msg)
            if not info:
                await msg.reply_text("❌ Could not read video metadata.", parse_mode=enums.ParseMode.HTML)
                return

            _, pos, saved_item = save_media_to_queue(info)
            entry = bulk_upload_buffers.get(uid)
            if not entry:
                entry = {"chat_id": msg.chat.id, "added": [], "task": None}
                bulk_upload_buffers[uid] = entry
            entry["added"].append((pos, saved_item))
            if entry.get("task") and not entry["task"].done():
                entry["task"].cancel()
            entry["task"] = asyncio.create_task(flush_bulk_yplay_upload(uid))
            return

        if not mode or not msg.text:
            return

        text = msg.text.strip()

        # --- /YStream -> Add Channel ---
        if mode == "ys_add":
            ok, err_msg, parsed = validate_channel_lines(text)
            if not ok or not parsed:
                await msg.reply_text(err_msg, parse_mode=enums.ParseMode.HTML)
                return
            name, url, logo = parsed
            chans = get_channels_sorted()
            if any(c.get("name", "").strip().lower() == name.lower() or c.get("url", "").strip() == url for c in chans):
                await msg.reply_text(
                    "⚠️ <b>Duplicate Channel</b>\nThis channel name or stream link already exists in /YStream.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            next_num = max([int(c.get("num") or 0) for c in chans], default=0) + 1
            new_ch = make_channel_obj(next_num, name, url, logo)
            probe_res = await asyncio.to_thread(probe_stream_url_sync, url)
            new_ch["status"] = "offline" if probe_res == "offline" else "online"
            chans.append(new_ch)
            write_json(CHANNELS_FILE, chans)
            broadcast_event("channels")
            bot_dialog_state.pop(uid, None)
            dash_txt, dash_kb = build_ystream_dashboard()
            await msg.reply_text(
                f"✅ <b>Channel #{next_num} ({html.escape(name)}) added!</b>\n\n{dash_txt}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=dash_kb,
            )
            return

        # --- /YStream -> Remove Channel (Single or Bulk) ---
        if mode == "ys_remove":
            tokens = [t for t in re.split(r"[\s,]+", text) if t]
            if not tokens or any(not t.isdigit() for t in tokens):
                await msg.reply_text(
                    "❌ <b>Invalid Channel Number</b>\nSend a valid number (e.g. <code>1</code>) or multiple numbers (e.g. <code>1 2 4</code>).",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            nums = list(dict.fromkeys(int(t) for t in tokens))
            chans = get_channels_sorted()
            existing_nums = {int(c.get("num") or 0) for c in chans}
            missing = [n for n in nums if n not in existing_nums]
            if missing:
                await msg.reply_text(
                    f"❌ <b>Channel Not Found:</b> <code>{', '.join(map(str, missing))}</code>\nNo channels were removed.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            updated = [c for c in chans if int(c.get("num") or 0) not in set(nums)]
            write_json(CHANNELS_FILE, updated)
            broadcast_event("channels")
            bot_dialog_state.pop(uid, None)
            dash_txt, dash_kb = build_ystream_dashboard()
            await msg.reply_text(
                f"✅ <b>Removed channel(s):</b> <code>{', '.join(map(str, nums))}</code>\n\n{dash_txt}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=dash_kb,
            )
            return

        # --- /YStream -> Edit Step 1 (Pick Number) ---
        if mode == "ys_edit_num":
            if not text.isdigit():
                await msg.reply_text("❌ Please enter a valid channel number (e.g. <code>1</code>).", parse_mode=enums.ParseMode.HTML)
                return
            target_num = int(text)
            chans = get_channels_sorted()
            ch = next((c for c in chans if int(c.get("num") or 0) == target_num), None)
            if not ch:
                await msg.reply_text(f"❌ Channel number <code>{target_num}</code> does not exist.", parse_mode=enums.ParseMode.HTML)
                return
            bot_dialog_state[uid] = {"mode": "ys_edit_data", "num": target_num}
            cur_block = f"{ch.get('name')}\n{ch.get('url')}\n{ch.get('logo')}"
            await msg.reply_text(
                f"<b>Editing Channel #{target_num}</b>\nCurrent details:\n<pre>{html.escape(cur_block)}</pre>\n\n"
                f"Send the updated details in the exact 3-line format:",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # --- /YStream -> Edit Step 2 (Submit 3 Lines) ---
        if mode == "ys_edit_data":
            target_num = int(st.get("num") or 0)
            ok, err_msg, parsed = validate_channel_lines(text)
            if not ok or not parsed:
                await msg.reply_text(err_msg, parse_mode=enums.ParseMode.HTML)
                return
            name, url, logo = parsed
            chans = get_channels_sorted()
            ch = next((c for c in chans if int(c.get("num") or 0) == target_num), None)
            if not ch:
                bot_dialog_state.pop(uid, None)
                await msg.reply_text("❌ Channel no longer exists.", parse_mode=enums.ParseMode.HTML)
                return

            if (
                ch.get("name", "").strip() == name
                and ch.get("url", "").strip() == url
                and ch.get("logo", "").strip() == logo
            ):
                await msg.reply_text(
                    "⚠️ <b>Duplicate Data Detected</b>\nThe submitted details are identical to the already saved channel details.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            for other in chans:
                if int(other.get("num") or 0) == target_num:
                    continue
                if other.get("name", "").strip().lower() == name.lower() or other.get("url", "").strip() == url:
                    await msg.reply_text(
                        "⚠️ <b>Duplicate Channel Data</b>\nAnother channel already uses this name or stream link.",
                        parse_mode=enums.ParseMode.HTML,
                    )
                    return

            ch["name"] = name
            ch["url"] = url
            ch["logo"] = logo
            ch["type"] = "dash" if ".mpd" in url.lower() else "hls"
            ch["short"] = re.sub(r"[^A-Za-z0-9]", "", name).upper()[:6] or f"CH{target_num}"
            ch["client_failed"] = False
            ch["failed_at"] = 0
            probe_res = await asyncio.to_thread(probe_stream_url_sync, url)
            ch["status"] = "offline" if probe_res == "offline" else "online"

            write_json(CHANNELS_FILE, chans)
            broadcast_event("channels")
            bot_dialog_state.pop(uid, None)
            dash_txt, dash_kb = build_ystream_dashboard()
            await msg.reply_text(
                f"✅ <b>Channel #{target_num} updated successfully!</b>\n\n{dash_txt}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=dash_kb,
            )
            return

        # --- /YPlay -> Remove File ---
        if mode == "yp_remove":
            tokens = [t for t in re.split(r"[\s,]+", text) if t]
            if not tokens or any(not t.isdigit() for t in tokens):
                await msg.reply_text(
                    "❌ <b>Invalid File Number</b>\nSend a valid number (e.g. <code>1</code>) or multiple numbers (e.g. <code>1 2 4</code>).",
                    parse_mode=enums.ParseMode.HTML,
                )
                return
            nums = list(dict.fromkeys(int(t) for t in tokens))
            items = get_library_items()
            invalid = [n for n in nums if n < 1 or n > len(items)]
            if invalid:
                await msg.reply_text(
                    f"❌ <b>File Number Not Found:</b> <code>{', '.join(map(str, invalid))}</code>\nNo files were removed.",
                    parse_mode=enums.ParseMode.HTML,
                )
                return

            remove_set = set(nums)
            kept = []
            removed_ids = []
            for idx, row in enumerate(items, start=1):
                if idx in remove_set:
                    removed_ids.append(row["id"])
                else:
                    kept.append(row)

            write_json(LIBRARY_FILE, kept)
            remove_item_from_all_device_progress(removed_ids)
            for rid in removed_ids:
                msg_cache.pop(rid, None)
                t = prewarm_tasks.pop(rid, None)
                if t:
                    t.cancel()
                await asyncio.to_thread(delete_item_cache_sync, rid)

            broadcast_event("library", {"removed_ids": removed_ids})
            bot_dialog_state.pop(uid, None)
            dash_txt, dash_kb = build_yplay_dashboard()
            await msg.reply_text(
                f"✅ <b>Removed file(s) & renumbered YPlay list:</b>\n\n{dash_txt}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=dash_kb,
            )
            return

        # --- /device -> Inspect Device Info ---
        if mode == "dev_info":
            if not text.isdigit():
                await msg.reply_text("❌ Enter a valid device number (e.g. <code>1</code>).", parse_mode=enums.ParseMode.HTML)
                return
            d_num = int(text)
            devices = get_devices_ordered()
            dev = next((d for d in devices if d.get("num") == d_num), None)
            if not dev:
                await msg.reply_text(f"❌ Device <code>#{d_num}</code> not found.", parse_mode=enums.ParseMode.HTML)
                return
            bot_dialog_state.pop(uid, None)
            act = dev.get("activity") or "idle"
            act_label = "Live TV" if act == "live" else ("YPlay" if act == "yplay" else "Idle")
            is_live_now = bool(dev.get("is_online"))
            play_label = "Currently Playing" if is_live_now else "Last Watched"
            info_lines = [
                f"Device Number  : #{dev.get('num')}",
                f"Device Name    : {dev.get('name')}",
                f"Device Type    : {dev.get('device_type') or classify_device_type(dev.get('device_id', ''), dev.get('user_agent', ''))}",
                f"Device ID      : {dev.get('device_id')}",
                f"Status         : {'Live Now 🟢' if is_live_now else 'Offline'}",
                f"Activity       : {act_label}",
                f"{play_label.ljust(14)} : {dev.get('playing') or 'None'}",
                f"First Visited  : {fmt_ist_full(float(dev.get('created_at') or 0))}",
                f"Last Active    : {fmt_ist_full(float(dev.get('last_seen') or 0))} ({fmt_ago(float(dev.get('last_seen') or 0))})",
            ]
            await msg.reply_text(
                f"<b>📱 Device Details</b>\n<pre>{html.escape(chr(10).join(info_lines))}</pre>",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Remove from List", callback_data=f"dev:del_id:{dev.get('device_id')}")],
                    [InlineKeyboardButton("⬅️ Back to Devices", callback_data="dev:refresh")],
                ]),
            )
            return

        # --- /device -> Remove Device Entry ---
        if mode == "dev_remove":
            if not text.isdigit():
                await msg.reply_text("❌ Enter a valid device number (e.g. <code>1</code>).", parse_mode=enums.ParseMode.HTML)
                return
            d_num = int(text)
            devices = get_devices_ordered()
            dev = next((d for d in devices if d.get("num") == d_num), None)
            if not dev:
                await msg.reply_text(f"❌ Device <code>#{d_num}</code> not found.", parse_mode=enums.ParseMode.HTML)
                return
            raw_devs = read_json(DEVICES_FILE, {})
            if isinstance(raw_devs, dict):
                raw_devs.pop(dev.get("device_id"), None)
                write_json(DEVICES_FILE, raw_devs)
            bot_dialog_state.pop(uid, None)
            dash_txt, dash_kb = build_device_dashboard_text()
            await msg.reply_text(
                f"🗑 <b>Removed {html.escape(dev.get('name') or 'Device')} from list</b>\n\n{dash_txt}",
                parse_mode=enums.ParseMode.HTML,
                reply_markup=dash_kb,
            )
            return
    except Exception as exc:
        print(f"[Interactive input error] {exc}")


@tg_client.on_callback_query()
async def on_callback(_, cq: CallbackQuery):
    try:
        if not authorize_user(cq.from_user, allow_claim=True):
            return
        uid = int(cq.from_user.id)
        data = str(cq.data or "")

        # --- /YStream Callbacks ---
        if data == "ys:refresh":
            await cq.answer("Checking offline streams...")
            bot_dialog_state.pop(uid, None)
            await live_check_channels(manual_refresh=True)
            txt, kb = build_ystream_dashboard()
            await safe_edit_message(cq.message, txt, kb)
            return

        if data == "ys:add":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "ys_add"}
            await cq.message.reply_text(
                "<b>Add Live TV Channel</b>\nSend channel details in exact 3-line format:\n\n"
                "<code>Channel Name\nLive Stream Link\nlogoname.webp</code>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data == "ys:remove":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "ys_remove"}
            await cq.message.reply_text(
                "<b>Remove Live TV Channel(s)</b>\nEnter the channel number (e.g. <code>1</code>) or multiple numbers (e.g. <code>1 2 4</code>):",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data == "ys:edit":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "ys_edit_num"}
            await cq.message.reply_text(
                "<b>Edit Live TV Channel</b>\nEnter the channel number you want to edit:",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # --- /YPlay Callbacks ---
        if data == "yp:refresh":
            await cq.answer("Refreshing YPlay...")
            bot_dialog_state.pop(uid, None)
            txt, kb = build_yplay_dashboard()
            await safe_edit_message(cq.message, txt, kb)
            return

        if data == "yp:add":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "yp_add"}
            await cq.message.reply_text(
                "<b>Add File to YPlay</b>\nSend or forward your video file(s) now.\n"
                "<i>Supports single or bulk video uploads. Non-video files will be rejected.</i>",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        if data == "yp:remove":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "yp_remove"}
            await cq.message.reply_text(
                "<b>Remove File(s) from YPlay</b>\nEnter the file number (e.g. <code>1</code>) or multiple numbers (e.g. <code>1 2 4</code>):",
                parse_mode=enums.ParseMode.HTML,
            )
            return

        # --- /device Callbacks ---
        if data == "dev:refresh":
            await cq.answer("Refreshing devices...")
            bot_dialog_state.pop(uid, None)
            txt, kb = build_device_dashboard_text()
            await safe_edit_message(cq.message, txt, kb)
            return

        if data == "dev:info_prompt":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "dev_info"}
            await cq.message.reply_text("Enter the device number to inspect:", parse_mode=enums.ParseMode.HTML)
            return

        if data == "dev:remove_prompt":
            await cq.answer()
            bot_dialog_state[uid] = {"mode": "dev_remove"}
            await cq.message.reply_text("Enter the device number to remove from list:", parse_mode=enums.ParseMode.HTML)
            return

        if data.startswith("dev:del_id:"):
            await cq.answer("Removed")
            dev_id = data.split(":", 2)[2]
            raw_devs = read_json(DEVICES_FILE, {})
            rec = raw_devs.pop(dev_id, None) if isinstance(raw_devs, dict) else None
            if isinstance(raw_devs, dict):
                write_json(DEVICES_FILE, raw_devs)
            txt, kb = build_device_dashboard_text()
            dname = rec.get("name") if isinstance(rec, dict) else dev_id
            await safe_edit_message(cq.message, f"🗑 <b>Removed {html.escape(str(dname))}</b>\n\n{txt}", kb)
            return

        # --- /status Refresh Callback ---
        if data == "st:refresh":
            await cq.answer("Refreshing status...")
            txt = await build_status_html()
            await safe_edit_message(
                cq.message,
                txt,
                InlineKeyboardMarkup([[InlineKeyboardButton("🔄 Refresh", callback_data="st:refresh")]]),
            )
    except Exception as exc:
        print(f"[Callback error] {exc}")


# =============================================================================
# FASTAPI LIFECYCLE & ENDPOINTS
# =============================================================================
@app.on_event("startup")
async def startup():
    await asyncio.to_thread(cleanup_orphan_tmps_sync)
    await tg_client.start()
    try:
        await tg_client.set_bot_commands([
            BotCommand("start", "Confirm bot status & menu"),
            BotCommand("ystream", "Manage Live TV channels"),
            BotCommand("yplay", "Manage YPlay movies & files"),
            BotCommand("device", "View live & past devices"),
            BotCommand("status", "Complete system status"),
            BotCommand("shutdown", "Emergency system shutdown"),
            BotCommand("resume", "Resume system playback"),
        ])
    except Exception as exc:
        print(f"[Bot commands error] {exc}")

    items = get_library_items()
    if items:
        trigger_prewarm(items[0])
    asyncio.create_task(zero_idle_channel_recovery_loop())


@app.on_event("shutdown")
async def shutdown():
    await kill_active_play_proc()
    for t in list(prewarm_tasks.values()):
        t.cancel()
    try:
        await tg_client.stop()
    except Exception:
        pass


@app.options("/{full_path:path}")
async def preflight_handler():
    return Response(
        status_code=200,
        headers={
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS, HEAD",
            "Access-Control-Allow-Headers": "*",
        },
    )


@app.get("/health")
async def health():
    items = get_library_items()
    chans = get_channels_sorted()
    tunnel = await asyncio.to_thread(get_active_tunnel_url)
    return {
        "status": "ok",
        "stopped": SYSTEM_STOPPED,
        "bot_connected": tg_client.is_connected,
        "yplay_count": len(items),
        "channel_count": len(chans),
        "tunnel_url": tunnel,
    }


@app.get("/api/events")
async def api_events(request: Request):
    q: asyncio.Queue = asyncio.Queue(maxsize=32)
    state_subscribers.add(q)

    async def event_stream():
        try:
            init_payload = json.dumps({"type": "init", "stopped": SYSTEM_STOPPED, "ts": time.time()})
            yield f"data: {init_payload}\n\n"
            while True:
                if await request.is_disconnected():
                    break
                try:
                    msg = await asyncio.wait_for(q.get(), timeout=15.0)
                    yield f"data: {msg}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            state_subscribers.discard(q)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


# --- Zero-Fail Device Activity Endpoints (Both GET /ping and POST /heartbeat) ---
@app.get("/api/device/ping")
async def device_ping(
    request: Request,
    device_id: str = "",
    model: str = "",
    platform: str = "",
    activity: str = "idle",
    playing: str = "",
):
    ua = str(request.headers.get("user-agent") or "")
    rec = record_device_activity(
        device_id=device_id,
        user_agent=ua,
        client_model=model,
        platform=platform,
        activity=activity,
        playing=playing,
    )
    return {
        "ok": bool(rec),
        "stopped": SYSTEM_STOPPED,
        "name": rec.get("name") if rec else "",
    }


@app.post("/api/device/heartbeat")
async def device_heartbeat(request: Request):
    try:
        raw_body = await request.body()
        body = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    raw_id = str(body.get("device_id") or request.query_params.get("device_id") or "")
    ua = str(request.headers.get("user-agent") or "")
    native_model = str(body.get("native_model") or body.get("model") or request.query_params.get("model") or "")
    platform = str(body.get("platform") or request.query_params.get("platform") or "")
    activity = str(body.get("activity") or request.query_params.get("activity") or "idle")
    playing = str(body.get("playing") or request.query_params.get("playing") or "")

    rec = record_device_activity(
        device_id=raw_id,
        user_agent=ua,
        client_model=native_model,
        platform=platform,
        activity=activity,
        playing=playing,
    )
    return {
        "ok": bool(rec),
        "stopped": SYSTEM_STOPPED,
        "name": rec.get("name") if rec else "",
    }


# --- Live TV Channels API ---
@app.get("/api/channels")
async def api_channels(request: Request, device_id: str = "", model: str = ""):
    if device_id:
        record_device_activity(
            device_id=device_id,
            user_agent=str(request.headers.get("user-agent") or ""),
            client_model=model,
        )
    chans = get_channels_sorted()
    return {
        "stopped": SYSTEM_STOPPED,
        "channels": chans,
    }


@app.post("/api/channels/{num}/report_error")
async def api_channel_report_error(num: int):
    await mark_channel_status(num, "offline", notify=True, from_client=True)
    return {"ok": True, "num": num, "status": "offline"}


# --- YPlay Library (With Isolated Per-Device Progress) ---
@app.get("/api/yplay/library")
async def yplay_library(request: Request, device_id: str = "", model: str = ""):
    clean_did = re.sub(r"[^a-zA-Z0-9_-]", "", str(device_id or ""))[:64]
    if clean_did:
        record_device_activity(
            device_id=clean_did,
            user_agent=str(request.headers.get("user-agent") or ""),
            client_model=model,
        )

    prog_all = read_json(PROGRESS_FILE, {})
    dev_prog = prog_all.get(clean_did, {}) if (clean_did and isinstance(prog_all, dict)) else {}
    if not isinstance(dev_prog, dict):
        dev_prog = {}

    items = get_library_items()
    enriched = []
    for idx, x in enumerate(items):
        row = dict(x)
        f_size = int(row.get("file_size") or 0)
        cur_dur = int(row.get("duration") or 0)
        if not row.get("probed") or (f_size > 20 * 1024 * 1024 and cur_dur < 30):
            trigger_prewarm(row)
        row["index"] = idx
        row["number"] = idx + 1
        row["quality"] = detect_quality(
            row.get("title", ""),
            int(row.get("width") or 0),
            int(row.get("height") or 0),
        )
        item_prog = dev_prog.get(str(row.get("id")), {})
        row["progress_sec"] = int(item_prog.get("position") or 0) if isinstance(item_prog, dict) else 0
        enriched.append(row)
    return {"stopped": SYSTEM_STOPPED, "count": len(enriched), "items": enriched}


@app.post("/api/yplay/progress")
async def yplay_save_progress(request: Request):
    try:
        raw_body = await request.body()
        body = json.loads(raw_body.decode("utf-8")) if raw_body else {}
        if not isinstance(body, dict):
            body = {}
    except Exception:
        body = {}

    raw_id = str(body.get("device_id") or request.query_params.get("device_id") or "")
    clean_did = re.sub(r"[^a-zA-Z0-9_-]", "", raw_id)[:64]
    item_id = str(body.get("id") or "")
    if len(clean_did) < 6 or not item_id:
        return {"ok": False}

    pos = max(0, int(body.get("position") or 0))
    explicit_seek = bool(body.get("explicit_seek", False))
    completed = bool(body.get("completed", False))

    _, item = find_yplay_item(item_id)
    if not item:
        return {"ok": False}

    # Keep device live telemetry updated while saving playback progress
    record_device_activity(
        device_id=clean_did,
        user_agent=str(request.headers.get("user-agent") or ""),
        client_model=str(body.get("model") or ""),
        activity="yplay",
        playing=str(item.get("title") or ""),
    )

    prog_all = read_json(PROGRESS_FILE, {})
    if not isinstance(prog_all, dict):
        prog_all = {}
    dev_map = prog_all.get(clean_did)
    if not isinstance(dev_map, dict):
        dev_map = {}

    prev_rec = dev_map.get(item_id, {})
    prev = int(prev_rec.get("position") or 0) if isinstance(prev_rec, dict) else 0
    dur = int(item.get("duration") or 0)

    if completed and dur > 30 and pos >= dur - 20:
        dev_map[item_id] = {"position": 0, "updated_at": int(time.time())}
    elif explicit_seek:
        dev_map[item_id] = {"position": pos, "updated_at": int(time.time())}
    else:
        if prev > 120 and pos < int(prev * 0.5):
            return {"ok": True, "progress_sec": prev, "ignored_regression": True}
        if pos > prev or abs(pos - prev) <= 30:
            dev_map[item_id] = {"position": pos, "updated_at": int(time.time())}

    prog_all[clean_did] = dev_map
    write_json(PROGRESS_FILE, prog_all)
    return {"ok": True}


@app.api_route("/api/yplay/stream/{item_ref}", methods=["GET", "HEAD"])
async def yplay_stream(item_ref: str, request: Request, device_id: str = "", model: str = ""):
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")

    _, item = find_yplay_item(item_ref)
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found in YPlay queue")

    if device_id and request.method == "GET":
        record_device_activity(
            device_id=device_id,
            user_agent=str(request.headers.get("user-agent") or ""),
            client_model=model,
            activity="yplay",
            playing=str(item.get("title") or ""),
        )

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

    async def continuous_gen():
        pos = start_byte
        retries = 0
        while pos <= end_byte:
            if SYSTEM_STOPPED or await request.is_disconnected():
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
                    retries = 0
                    continue

            stream_block = b_idx
            buf = bytearray()
            progressed = False

            try:
                refresh_msg = await get_tg_message(item, force_refresh=(retries > 0))
                async for chunk in tg_client.stream_media(refresh_msg, offset=b_idx):
                    if SYSTEM_STOPPED or await request.is_disconnected():
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
                                progressed = True
                                retries = 0

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
                        progressed = True
                        retries = 0
            except Exception as exc:
                msg_cache.pop(item_id, None)
                retries += 1
                if retries > 4:
                    print(f"[Stream abort {item_id} at {pos}] {exc}")
                    break
                await asyncio.sleep(min(2.0, 0.4 * retries))
                continue

            if not progressed and pos <= end_byte:
                retries += 1
                if retries > 4:
                    break
                await asyncio.sleep(0.4)

        asyncio.create_task(asyncio.to_thread(prune_cache_sync))

    return StreamingResponse(continuous_gen(), status_code=status_code, headers=headers, media_type=mime)


@app.api_route("/api/yplay/play/{item_ref}", methods=["GET", "HEAD"])
async def yplay_play_stream(
    item_ref: str,
    request: Request,
    t: float = 0.0,
    q: str = "orig",
    a: int = 0,
    device_id: str = "",
    model: str = "",
):
    global active_play_proc
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")

    _, item = find_yplay_item(item_ref)
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found")

    if request.method == "HEAD":
        return Response(status_code=200, headers={"Content-Type": "video/mp4", "Cache-Control": "no-store"})

    if device_id:
        record_device_activity(
            device_id=device_id,
            user_agent=str(request.headers.get("user-agent") or ""),
            client_model=model,
            activity="yplay",
            playing=str(item.get("title") or ""),
        )

    item_id = item["id"]
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
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{item_id}",
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

    async with play_proc_lock:
        if active_play_proc and active_play_proc.returncode is None:
            try:
                active_play_proc.kill()
                await active_play_proc.wait()
            except Exception:
                pass
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


@app.get("/api/yplay/sub/{item_ref}/{sub_idx}.vtt")
async def yplay_sub_vtt(
    item_ref: str,
    sub_idx: int,
    t: float = 0.0,
    dur: float = 180.0,
):
    if SYSTEM_STOPPED:
        raise HTTPException(status_code=503, detail="System shutdown")

    _, item = find_yplay_item(item_ref)
    if not item:
        raise HTTPException(status_code=404, detail="Movie not found")

    item_id = item["id"]
    start_sec = max(0.0, float(t or 0.0))
    win_sec = min(240.0, max(30.0, float(dur or 180.0)))
    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error",
        "-ss", f"{start_sec:.2f}",
        "-i", f"http://127.0.0.1:8000/api/yplay/stream/{item_id}",
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
