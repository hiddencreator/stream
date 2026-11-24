import asyncio
import json
import os
import re
import shutil
import subprocess 
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pyrogram import Client, filters
from pyrogram.types import (
    BotCommand,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "6"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")
DEFAULT_SUNTV_URL = "https://d2px1d6ssc60fw.cloudfront.net/SunTV/SunTV-HDR10-IN-index.m3u8"

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
CACHE_DIR = Path("/tmp/yplay_chunks")
HLS_DIR = Path("/dev/shm/suntv_hls")
LIBRARY_FILE = DATA_DIR / "yplay_library.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
SETTINGS_FILE = DATA_DIR / "settings.json"
SESSION_FILE = DATA_DIR / "ystream_bot"

CHUNK_SIZE = 1024 * 1024  # 1 MiB Telegram chunk

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

DEFAULT_SETTINGS = {
    "prewarm": True,
    "max_cache_gb": 3,
    "auto_reply": True,
}

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
last_error: str = ""
msg_cache: dict[str, Message] = {}
prewarm_tasks: dict[str, asyncio.Task] = {}


def read_json(path: Path, default):
    if not path.exists():
        path.write_text(json.dumps(default, indent=2), encoding="utf-8")
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def write_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def get_settings() -> dict:
    data = read_json(SETTINGS_FILE, DEFAULT_SETTINGS)
    merged = dict(DEFAULT_SETTINGS)
    if isinstance(data, dict):
        merged.update(data)
    return merged


def update_settings(**kwargs) -> dict:
    s = get_settings()
    s.update(kwargs)
    write_json(SETTINGS_FILE, s)
    return s


read_json(LIBRARY_FILE, [])
read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
read_json(SETTINGS_FILE, DEFAULT_SETTINGS)


def get_active_tunnel_url() -> str:
    for cmd in (
        ["journalctl", "-u", "ystream-tunnel", "-n", "100", "--no-pager"],
        ["sudo", "-n", "journalctl", "-u", "ystream-tunnel", "-n", "100", "--no-pager"],
    ):
        try:
            out = subprocess.check_output(cmd, text=True, stderr=subprocess.DEVNULL)
            matches = re.findall(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", out)
            if matches:
                return matches[-1]
        except Exception:
            pass
    return ""


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
    mb = size / (1024 * 1024)
    return f"{mb:.1f} MB"


def get_media_info(msg: Message) -> Optional[dict]:
    media = msg.video or msg.document or msg.animation
    if not media:
        return None
    name = getattr(media, "file_name", None) or msg.caption or f"Movie_{msg.id}.mp4"
    title = str(name).strip()
    mime = getattr(media, "mime_type", None) or "video/mp4"
    size = getattr(media, "file_size", None) or 0
    duration = getattr(media, "duration", None) or 0
    width = int(getattr(media, "width", 0) or 0)
    height = int(getattr(media, "height", 0) or 0)
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
    total = sum(f.stat().st_size for f in files)
    return len(files), total


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
    max_gb = int(get_settings().get("max_cache_gb", 3) or 3)
    max_bytes = max_gb * 1024 * 1024 * 1024
    files = list(CACHE_DIR.glob("*/*.blk"))
    total = sum(f.stat().st_size for f in files if f.exists())
    if total <= max_bytes:
        return
    files.sort(key=lambda f: f.stat().st_mtime)
    for f in files:
        if total <= int(max_bytes * 0.75):
            break
        try:
            sz = f.stat().st_size
            f.unlink()
            total -= sz
        except Exception:
            pass


async def clear_queue_and_cache():
    for task in list(prewarm_tasks.values()):
        task.cancel()
    prewarm_tasks.clear()
    msg_cache.clear()
    write_json(LIBRARY_FILE, [])
    await asyncio.to_thread(clear_all_cache_sync)


async def get_tg_message(item: dict, force_refresh: bool = False) -> Message:
    item_id = item["id"]
    if not force_refresh and item_id in msg_cache:
        return msg_cache[item_id]
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

        head_count = min(3, total_blocks)
        if not all(block_path(item_id, b).exists() for b in range(head_count)):
            await cache_span(item, 0, head_count)

        if total_blocks > 5:
            tail_start = total_blocks - 2
            if not all(block_path(item_id, b).exists() for b in range(tail_start, total_blocks)):
                await cache_span(item, tail_start, 2)

        await asyncio.to_thread(prune_cache_sync)
    except asyncio.CancelledError:
        pass
    except Exception as exc:
        print(f"[Prewarm {item_id}] {exc}")
    finally:
        prewarm_tasks.pop(item_id, None)


def trigger_prewarm(item: dict):
    if not get_settings().get("prewarm", True):
        return
    item_id = item["id"]
    if item_id in prewarm_tasks:
        return
    if block_path(item_id, 0).exists():
        return
    prewarm_tasks[item_id] = asyncio.create_task(prewarm_head_tail(item))


def save_media_to_queue(item: dict) -> tuple[bool, int]:
    items = read_json(LIBRARY_FILE, [])
    for row in items:
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            trigger_prewarm(row)
            return False, len(items)
    items.append(item)
    write_json(LIBRARY_FILE, items)
    trigger_prewarm(item)
    return True, len(items)


# --- Telegram Bot Menus & Keyboards ---
def main_menu_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("📋 Queue List", callback_data="cb:list"),
            InlineKeyboardButton("⚙️ Settings", callback_data="cb:settings"),
        ],
        [
            InlineKeyboardButton("📊 Server Status", callback_data="cb:status"),
            InlineKeyboardButton("🗑️ Clear All", callback_data="cb:clear_ask"),
        ],
    ])


def settings_menu_markup() -> InlineKeyboardMarkup:
    s = get_settings()
    prewarm_lbl = "⚡ Pre-warm: ON" if s.get("prewarm", True) else "⚡ Pre-warm: OFF"
    reply_lbl = "💬 Upload Reply: ON" if s.get("auto_reply", True) else "💬 Upload Reply: OFF"
    cache_gb = int(s.get("max_cache_gb", 3) or 3)
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(prewarm_lbl, callback_data="cb:toggle_prewarm"),
            InlineKeyboardButton(f"💾 Cache Limit: {cache_gb} GB", callback_data="cb:cycle_cache"),
        ],
        [
            InlineKeyboardButton(reply_lbl, callback_data="cb:toggle_reply"),
            InlineKeyboardButton("🧹 Clear Cache Only", callback_data="cb:clear_cache_only"),
        ],
        [
            InlineKeyboardButton("🔄 Restart Sun TV Relay", callback_data="cb:restart_relay"),
        ],
        [
            InlineKeyboardButton("⬅️ Back to Menu", callback_data="cb:menu"),
        ],
    ])


def clear_confirm_markup() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Yes, Delete All", callback_data="cb:clear_confirm"),
            InlineKeyboardButton("❌ Cancel", callback_data="cb:menu"),
        ]
    ])


def back_menu_markup(extra_btn: Optional[InlineKeyboardButton] = None) -> InlineKeyboardMarkup:
    rows = []
    if extra_btn:
        rows.append([extra_btn])
    rows.append([InlineKeyboardButton("⬅️ Back to Menu", callback_data="cb:menu")])
    return InlineKeyboardMarkup(rows)


def build_queue_text() -> str:
    items = read_json(LIBRARY_FILE, [])
    if not items:
        return "📋 **YPlay Queue is Empty**\n\nForward or send any video/movie to add it to `#1`."
    lines = []
    start_idx = max(0, len(items) - 20)
    for i in range(start_idx, len(items)):
        x = items[i]
        q = x.get("quality") or detect_quality(x.get("title", ""), int(x.get("width") or 0), int(x.get("height") or 0))
        sz = fmt_bytes(int(x.get("file_size") or 0))
        lines.append(f"`#{i + 1}` • **{x.get('title')}** (`{q}` • `{sz}`)")
    return f"📋 **YPlay Queue ({len(items)} items)**\n\n" + "\n".join(lines) + "\n\nUse `/del <number>` to remove one, or `/clear` to wipe all."


async def build_status_text() -> str:
    items = read_json(LIBRARY_FILE, [])
    s = get_settings()
    blk_cnt, blk_bytes = await asyncio.to_thread(get_cache_stats_sync)
    tunnel = get_active_tunnel_url() or "Starting / Not detected"
    relay_ok = ffmpeg_proc is not None and ffmpeg_proc.poll() is None
    return (
        "📊 **YStream Server Status**\n\n"
        f"• **Bot:** Connected\n"
        f"• **YPlay Queue:** `{len(items)}` movies\n"
        f"• **SSD Cache:** `{fmt_bytes(blk_bytes)}` (`{blk_cnt}` chunks / `{s.get('max_cache_gb', 3)} GB` limit)\n"
        f"• **Pre-warm (5 MB):** `{'ON' if s.get('prewarm', True) else 'OFF'}`\n"
        f"• **Sun TV Relay:** `{'Active' if relay_ok else 'Inactive'}`\n"
        f"• **Tunnel:** `{tunnel}`"
    )


def build_settings_text() -> str:
    return (
        "⚙️ **YStream Bot Settings & Options**\n\n"
        "• **Pre-warm:** Automatically caches first 3 MB + last 2 MB on upload for fast start.\n"
        "• **Cache Limit:** Auto-cleans oldest chunks once SSD limit is reached.\n"
        "• **Upload Reply:** Send confirmation reply whenever a video is queued."
    )


def build_menu_text() -> str:
    items = read_json(LIBRARY_FILE, [])
    return (
        "🎬 **YPlay Control Menu**\n\n"
        f"Movies in queue: `{len(items)}`\n"
        "Forward any movie or video directly here to stream on `ystream.pages.dev` & TV App."
    )


async def safe_edit_msg(msg: Message, text: str, reply_markup: InlineKeyboardMarkup):
    try:
        await msg.edit_text(text, reply_markup=reply_markup)
    except Exception:
        pass


@tg_client.on_message(filters.command(["start", "menu"]))
async def cmd_menu(_, msg: Message):
    await msg.reply_text(build_menu_text(), reply_markup=main_menu_markup())


@tg_client.on_message(filters.command("settings"))
async def cmd_settings(_, msg: Message):
    await msg.reply_text(build_settings_text(), reply_markup=settings_menu_markup())


@tg_client.on_message(filters.command("status"))
async def cmd_status(_, msg: Message):
    txt = await build_status_text()
    await msg.reply_text(
        txt,
        reply_markup=back_menu_markup(InlineKeyboardButton("🔄 Refresh Status", callback_data="cb:status")),
    )


@tg_client.on_message(filters.command("list"))
async def cmd_list(_, msg: Message):
    await msg.reply_text(
        build_queue_text(),
        reply_markup=back_menu_markup(InlineKeyboardButton("🗑️ Clear All", callback_data="cb:clear_ask")),
    )


@tg_client.on_message(filters.command("del"))
async def cmd_del(_, msg: Message):
    parts = (msg.text or "").strip().split()
    if len(parts) < 2 or not parts[1].isdigit():
        await msg.reply_text("Use: `/del <number>` (example: `/del 1`)")
        return
    idx = int(parts[1]) - 1
    items = read_json(LIBRARY_FILE, [])
    if idx < 0 or idx >= len(items):
        await msg.reply_text("Invalid number. Check `/list`.")
        return
    removed = items.pop(idx)
    write_json(LIBRARY_FILE, items)
    msg_cache.pop(removed["id"], None)
    task = prewarm_tasks.pop(removed["id"], None)
    if task:
        task.cancel()
    shutil.rmtree(safe_item_dir(removed["id"]), ignore_errors=True)
    await msg.reply_text(f"🗑️ Removed: **{removed.get('title')}**", reply_markup=main_menu_markup())


@tg_client.on_message(filters.command("clear"))
async def cmd_clear(_, msg: Message):
    await clear_queue_and_cache()
    await msg.reply_text(
        "✅ **All Old Videos & Cache Cleared!**\n\nQueue is now empty. Next video you send will start at `#1`.",
        reply_markup=main_menu_markup(),
    )


@tg_client.on_message(filters.command("add"))
async def cmd_add(_, msg: Message):
    target = msg.reply_to_message or msg
    info = get_media_info(target)
    if not info:
        await msg.reply_text("Reply to a video file with `/add`.")
        return
    added, total = save_media_to_queue(info)
    await msg.reply_text(
        f"{'✅ Queued' if added else 'Already in queue'} `#{total}` (`{info['quality']}` • `{fmt_bytes(info['file_size'])}`):\n**{info['title']}**"
    )


@tg_client.on_message(filters.video | filters.document | filters.animation)
async def on_file(_, msg: Message):
    info = get_media_info(msg)
    if not info:
        return
    added, total = save_media_to_queue(info)
    if added and get_settings().get("auto_reply", True):
        await msg.reply_text(
            f"✅ Queued `#{total}` • `{info['quality']}` • `{fmt_bytes(info['file_size'])}`\n**{info['title']}**"
        )


@tg_client.on_callback_query()
async def on_callback(_, cq: CallbackQuery):
    data = cq.data or ""
    if data == "cb:menu":
        await cq.answer()
        await safe_edit_msg(cq.message, build_menu_text(), main_menu_markup())
    elif data == "cb:list":
        await cq.answer()
        await safe_edit_msg(
            cq.message,
            build_queue_text(),
            back_menu_markup(InlineKeyboardButton("🗑️ Clear All", callback_data="cb:clear_ask")),
        )
    elif data == "cb:settings":
        await cq.answer()
        await safe_edit_msg(cq.message, build_settings_text(), settings_menu_markup())
    elif data == "cb:status":
        await cq.answer("Refreshing status...")
        txt = await build_status_text()
        await safe_edit_msg(
            cq.message,
            txt,
            back_menu_markup(InlineKeyboardButton("🔄 Refresh Status", callback_data="cb:status")),
        )
    elif data == "cb:toggle_prewarm":
        cur = bool(get_settings().get("prewarm", True))
        update_settings(prewarm=not cur)
        await cq.answer(f"Pre-warm {'OFF' if cur else 'ON'}")
        await safe_edit_msg(cq.message, build_settings_text(), settings_menu_markup())
    elif data == "cb:toggle_reply":
        cur = bool(get_settings().get("auto_reply", True))
        update_settings(auto_reply=not cur)
        await cq.answer(f"Upload Reply {'OFF' if cur else 'ON'}")
        await safe_edit_msg(cq.message, build_settings_text(), settings_menu_markup())
    elif data == "cb:cycle_cache":
        cur_gb = int(get_settings().get("max_cache_gb", 3) or 3)
        next_gb = 3 if cur_gb == 1 else (5 if cur_gb == 3 else 1)
        update_settings(max_cache_gb=next_gb)
        await asyncio.to_thread(prune_cache_sync)
        await cq.answer(f"Cache limit set to {next_gb} GB")
        await safe_edit_msg(cq.message, build_settings_text(), settings_menu_markup())
    elif data == "cb:clear_cache_only":
        await asyncio.to_thread(clear_all_cache_sync)
        await cq.answer("SSD cache cleared (queue kept)!", show_alert=True)
        await safe_edit_msg(cq.message, build_settings_text(), settings_menu_markup())
    elif data == "cb:restart_relay":
        await cq.answer("Restarting Sun TV relay...")
        global ffmpeg_proc
        if ffmpeg_proc and ffmpeg_proc.poll() is None:
            ffmpeg_proc.terminate()
        ok = await start_suntv_relay()
        txt = await build_status_text()
        await safe_edit_msg(
            cq.message,
            txt + f"\n\nRelay restart: {'✅ Ready' if ok else '⚠️ Source blocked/error'}",
            back_menu_markup(InlineKeyboardButton("⚙️ Settings", callback_data="cb:settings")),
        )
    elif data == "cb:clear_ask":
        items = read_json(LIBRARY_FILE, [])
        await cq.answer()
        await safe_edit_msg(
            cq.message,
            f"⚠️ **Clear All `{len(items)}` Movies & Cache?**\n\nThis will delete every queued movie and cached chunk. Next upload will start from `#1`.",
            clear_confirm_markup(),
        )
    elif data == "cb:clear_confirm":
        await clear_queue_and_cache()
        await cq.answer("All videos & cache cleared!", show_alert=True)
        await safe_edit_msg(
            cq.message,
            "✅ **YPlay Queue & Cache Cleared!**\n\nNext upload will start from `#1`.",
            main_menu_markup(),
        )


async def start_suntv_relay():
    global ffmpeg_proc, last_error
    channels = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
    suntv = next((c for c in channels if c.get("id") == "suntv"), None)
    url = (suntv or {}).get("source_url") or os.getenv("SUNTV_SOURCE_URL") or DEFAULT_SUNTV_URL

    m3u8 = HLS_DIR / "index.m3u8"
    if ffmpeg_proc and ffmpeg_proc.poll() is None and m3u8.exists() and (time.time() - m3u8.stat().st_mtime) < 10:
        return True

    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        ffmpeg_proc.terminate()
        try:
            ffmpeg_proc.wait(timeout=2)
        except Exception:
            ffmpeg_proc.kill()

    for f in HLS_DIR.glob("*"):
        try:
            f.unlink()
        except Exception:
            pass

    cmd = [
        "ffmpeg", "-nostdin", "-loglevel", "error",
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2",
        "-i", url,
        "-vf", "fps=25,scale=-2:720,format=yuv420p",
        "-c:v", "libx264", "-preset", "ultrafast", "-tune", "zerolatency",
        "-crf", "22", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0",
        "-c:a", "aac", "-b:a", "128k", "-ac", "2",
        "-f", "hls", "-hls_time", "2", "-hls_list_size", "5",
        "-hls_flags", "delete_segments+append_list+independent_segments",
        str(m3u8),
    ]
    try:
        ffmpeg_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except Exception as e:
        last_error = f"FFmpeg start error: {e}"
        return False

    for _ in range(24):
        if m3u8.exists() and len(list(HLS_DIR.glob("*.ts"))) >= 2:
            last_error = ""
            return True
        if ffmpeg_proc.poll() is not None:
            err = ffmpeg_proc.stderr.read().strip() if ffmpeg_proc.stderr else "FFmpeg stopped"
            last_error = err
            return False
        await asyncio.sleep(0.25)

    last_error = "Sun TV stream timeout"
    return False


async def relay_watcher():
    while True:
        try:
            await start_suntv_relay()
        except Exception as e:
            global last_error
            last_error = str(e)
        await asyncio.sleep(6)


@app.on_event("startup")
async def startup():
    await tg_client.start()
    try:
        await tg_client.set_bot_commands([
            BotCommand("menu", "Open Bot Menu & Options"),
            BotCommand("list", "Show YPlay Movie Queue"),
            BotCommand("clear", "Delete All Movies & Clear Cache"),
            BotCommand("settings", "Bot & Cache Settings"),
            BotCommand("status", "Server & SSD Status"),
        ])
    except Exception as exc:
        print(f"[Bot commands error] {exc}")

    items = read_json(LIBRARY_FILE, [])
    if items:
        trigger_prewarm(items[0])
    asyncio.create_task(relay_watcher())


@app.on_event("shutdown")
async def shutdown():
    for t in list(prewarm_tasks.values()):
        t.cancel()
    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        ffmpeg_proc.terminate()
    try:
        await tg_client.stop()
    except Exception:
        pass


@app.get("/health")
async def health():
    items = read_json(LIBRARY_FILE, [])
    return {
        "status": "ok",
        "bot_connected": tg_client.is_connected,
        "yplay_count": len(items),
        "tunnel_url": get_active_tunnel_url(),
        "suntv_active": ffmpeg_proc is not None and ffmpeg_proc.poll() is None,
        "suntv_error": last_error,
    }


@app.get("/api/yplay/library")
async def yplay_library():
    items = read_json(LIBRARY_FILE, [])
    enriched = []
    for idx, x in enumerate(items):
        row = dict(x)
        row["index"] = idx
        if not row.get("quality"):
            row["quality"] = detect_quality(
                row.get("title", ""),
                int(row.get("width") or 0),
                int(row.get("height") or 0),
            )
        enriched.append(row)
    return {"count": len(enriched), "items": enriched}


@app.get("/api/yplay/stream/{index}")
async def yplay_stream(index: int, request: Request):
    items = read_json(LIBRARY_FILE, [])
    if index < 0 or index >= len(items):
        raise HTTPException(status_code=404, detail="Movie not found in YPlay queue")

    item = items[index]
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

    async def continuous_gen():
        pos = start_byte
        while pos <= end_byte:
            if await request.is_disconnected():
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
                    if await request.is_disconnected():
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

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Cache-Control": "no-cache",
        "Content-Length": str(length),
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        return StreamingResponse(continuous_gen(), status_code=206, headers=headers, media_type=mime)

    return StreamingResponse(continuous_gen(), status_code=200, headers=headers, media_type=mime)


@app.get("/live/suntv/index.m3u8")
async def suntv_m3u8():
    ok = await start_suntv_relay()
    m3u8 = HLS_DIR / "index.m3u8"
    if not ok or not m3u8.exists():
        raise HTTPException(status_code=503, detail=last_error or "Sun TV relay not ready")
    return FileResponse(str(m3u8), media_type="application/vnd.apple.mpegurl", headers={"Cache-Control": "no-store"})


@app.get("/live/suntv/{seg}")
async def suntv_ts(seg: str):
    if "/" in seg or ".." in seg or not seg.endswith(".ts"):
        raise HTTPException(status_code=400, detail="Bad segment")
    p = HLS_DIR / seg
    if not p.exists():
        raise HTTPException(status_code=404, detail="Segment not found")
    return FileResponse(str(p), media_type="video/mp2t", headers={"Cache-Control": "no-cache"})
