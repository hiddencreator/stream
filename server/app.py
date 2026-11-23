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
from pyrogram.types import Message

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
SESSION_FILE = DATA_DIR / "ystream_bot"

CHUNK_SIZE = 1024 * 1024  # Telegram native 1 MiB chunk
MAX_CACHE_BYTES = 3 * 1024 * 1024 * 1024  # Keep SSD cache under 3 GB

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
last_error: str = ""
msg_cache: dict[str, Message] = {}
block_locks: dict[tuple[str, int], asyncio.Lock] = {}
lookahead_tasks: dict[tuple[str, int], asyncio.Task] = {}
lookahead_sem = asyncio.Semaphore(2)


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


read_json(LIBRARY_FILE, [])
read_json(CHANNELS_FILE, DEFAULT_CHANNELS)


def get_active_tunnel_url() -> str:
    try:
        out = subprocess.check_output(
            ["journalctl", "-u", "ystream-tunnel", "-n", "100", "--no-pager"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        matches = re.findall(r"https://[a-zA-Z0-9-]+\.trycloudflare\.com", out)
        if matches:
            return matches[-1]
    except Exception:
        pass
    return ""


def get_media_info(msg: Message) -> Optional[dict]:
    media = msg.video or msg.document or msg.animation
    if not media:
        return None
    name = getattr(media, "file_name", None) or msg.caption or f"Movie_{msg.id}.mp4"
    mime = getattr(media, "mime_type", None) or "video/mp4"
    size = getattr(media, "file_size", None) or 0
    duration = getattr(media, "duration", None) or 0
    return {
        "id": f"{msg.chat.id}_{msg.id}",
        "chat_id": msg.chat.id,
        "message_id": msg.id,
        "title": str(name).strip(),
        "mime_type": mime,
        "file_size": int(size),
        "duration": int(duration),
        "added_at": int(time.time()),
    }


def safe_item_dir(item_id: str) -> Path:
    safe = "".join(c for c in item_id if c.isalnum() or c in ("_", "-"))
    p = CACHE_DIR / safe
    p.mkdir(parents=True, exist_ok=True)
    return p


def block_path(item_id: str, b_idx: int) -> Path:
    return safe_item_dir(item_id) / f"{b_idx}.blk"


def prune_cache_sync():
    files = list(CACHE_DIR.glob("*/*.blk"))
    total = sum(f.stat().st_size for f in files if f.exists())
    if total <= MAX_CACHE_BYTES:
        return
    files.sort(key=lambda f: f.stat().st_mtime)
    for f in files:
        if total <= int(MAX_CACHE_BYTES * 0.75):
            break
        try:
            sz = f.stat().st_size
            f.unlink()
            total -= sz
        except Exception:
            pass


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


async def fetch_block(item: dict, b_idx: int) -> bytes:
    item_id = item["id"]
    b_path = block_path(item_id, b_idx)
    if b_path.exists():
        return await asyncio.to_thread(b_path.read_bytes)

    key = (item_id, b_idx)
    lock = block_locks.get(key)
    if lock is None:
        lock = asyncio.Lock()
        block_locks[key] = lock

    async with lock:
        if b_path.exists():
            return await asyncio.to_thread(b_path.read_bytes)

        for attempt in range(2):
            try:
                msg = await get_tg_message(item, force_refresh=(attempt == 1))
                data = bytearray()
                async for chunk in tg_client.stream_media(msg, limit=1, offset=b_idx):
                    data.extend(chunk)
                    break
                raw = bytes(data)
                if raw:
                    tmp_path = b_path.with_suffix(".tmp")
                    await asyncio.to_thread(tmp_path.write_bytes, raw)
                    await asyncio.to_thread(os.replace, tmp_path, b_path)
                    asyncio.create_task(asyncio.to_thread(prune_cache_sync))
                return raw
            except Exception:
                msg_cache.pop(item_id, None)
                if attempt == 1:
                    raise
        return b""


async def lookahead_worker(item: dict, b_idx: int):
    key = (item["id"], b_idx)
    try:
        async with lookahead_sem:
            if not block_path(item["id"], b_idx).exists():
                await fetch_block(item, b_idx)
    except Exception:
        pass
    finally:
        lookahead_tasks.pop(key, None)


def schedule_lookahead(item: dict, start_block: int, total_blocks: int, count: int = 2):
    item_id = item["id"]
    for b in range(start_block, min(total_blocks, start_block + count)):
        key = (item_id, b)
        if key in lookahead_tasks or block_path(item_id, b).exists():
            continue
        if len(lookahead_tasks) >= 4:
            break
        lookahead_tasks[key] = asyncio.create_task(lookahead_worker(item, b))


def save_media_to_queue(item: dict) -> tuple[bool, int]:
    items = read_json(LIBRARY_FILE, [])
    for row in items:
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            schedule_lookahead(row, 0, 2, count=2)
            return False, len(items)
    items.append(item)
    write_json(LIBRARY_FILE, items)
    schedule_lookahead(item, 0, 2, count=2)
    return True, len(items)


@tg_client.on_message(filters.command("start"))
async def cmd_start(_, msg: Message):
    await msg.reply_text(
        "YPlay Ready.\n"
        "Forward or send movies/episodes — they stream on-demand instantly like YouTube."
    )


@tg_client.on_message(filters.command("list"))
async def cmd_list(_, msg: Message):
    items = read_json(LIBRARY_FILE, [])
    if not items:
        await msg.reply_text("YPlay queue is empty.")
        return
    lines = [f"{i + 1}. {x.get('title')}" for i, x in enumerate(items)]
    await msg.reply_text("YPlay Queue:\n" + "\n".join(lines[-25:]))


@tg_client.on_message(filters.command("del"))
async def cmd_del(_, msg: Message):
    parts = (msg.text or "").strip().split()
    if len(parts) < 2 or not parts[1].isdigit():
        await msg.reply_text("Use: /del <number>")
        return
    idx = int(parts[1]) - 1
    items = read_json(LIBRARY_FILE, [])
    if idx < 0 or idx >= len(items):
        await msg.reply_text("Invalid number.")
        return
    removed = items.pop(idx)
    write_json(LIBRARY_FILE, items)
    msg_cache.pop(removed["id"], None)
    shutil.rmtree(safe_item_dir(removed["id"]), ignore_errors=True)
    await msg.reply_text(f"Removed: {removed.get('title')}")


@tg_client.on_message(filters.command("add"))
async def cmd_add(_, msg: Message):
    target = msg.reply_to_message or msg
    info = get_media_info(target)
    if not info:
        await msg.reply_text("Reply to a video file with /add.")
        return
    added, total = save_media_to_queue(info)
    await msg.reply_text(f"{'Ready in queue' if added else 'Already in queue'} #{total}: {info['title']}")


@tg_client.on_message(filters.video | filters.document | filters.animation)
async def on_file(_, msg: Message):
    info = get_media_info(msg)
    if not info:
        return
    added, total = save_media_to_queue(info)
    if added:
        await msg.reply_text(f"Queued #{total} (Instant Stream Ready): {info['title']}")


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
    items = read_json(LIBRARY_FILE, [])
    if items:
        schedule_lookahead(items[0], 0, 2, count=1)
    asyncio.create_task(relay_watcher())


@app.on_event("shutdown")
async def shutdown():
    for t in list(lookahead_tasks.values()):
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
        enriched.append(row)
    return {"count": len(enriched), "items": enriched}


@app.get("/api/yplay/stream/{index}")
async def yplay_stream(index: int, request: Request):
    items = read_json(LIBRARY_FILE, [])
    if index < 0 or index >= len(items):
        raise HTTPException(status_code=404, detail="Movie not found in YPlay queue")

    item = items[index]
    msg = await get_tg_message(item)
    media = msg.video or msg.document or msg.animation
    total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
    mime = item.get("mime_type") or getattr(media, "mime_type", None) or "video/mp4"

    if total_size <= 0:
        raise HTTPException(status_code=500, detail="Unknown media size")

    total_blocks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
    range_header = request.headers.get("range")

    start_byte = 0
    end_byte = min(total_size - 1, (8 * CHUNK_SIZE) - 1)

    if range_header and "=" in range_header:
        try:
            _, rng = range_header.strip().split("=", 1)
            s_part, e_part = rng.split("-", 1)
            if s_part:
                start_byte = max(0, int(s_part))
            if e_part:
                end_byte = min(total_size - 1, int(e_part))
            else:
                end_byte = min(total_size - 1, start_byte + (8 * CHUNK_SIZE) - 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid range")

    if start_byte > end_byte or start_byte >= total_size:
        raise HTTPException(status_code=416, detail="Invalid range bounds")

    length = end_byte - start_byte + 1

    async def block_stream_gen():
        pos = start_byte
        while pos <= end_byte:
            if await request.is_disconnected():
                break
            b_idx = pos // CHUNK_SIZE
            b_off = pos % CHUNK_SIZE
            # Fetch ONLY next 2 MB ahead of playhead (YouTube-style buffer)
            schedule_lookahead(item, b_idx + 1, total_blocks, count=2)
            raw = await fetch_block(item, b_idx)
            if not raw or b_off >= len(raw):
                break
            take = min(len(raw) - b_off, end_byte - pos + 1)
            yield raw[b_off : b_off + take]
            pos += take

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Cache-Control": "no-cache",
        "Content-Length": str(length),
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        return StreamingResponse(block_stream_gen(), status_code=206, headers=headers, media_type=mime)

    return StreamingResponse(block_stream_gen(), status_code=200, headers=headers, media_type=mime)


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
