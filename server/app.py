import asyncio
import json
import os
import subprocess
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, StreamingResponse
from pyrogram import Client, filters
from pyrogram.types import Message

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "6"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
HLS_DIR = Path("/dev/shm/suntv_hls")
LIBRARY_FILE = DATA_DIR / "yplay_library.json"
CHANNELS_FILE = DATA_DIR / "channels.json"
SESSION_FILE = DATA_DIR / "ystream_bot"

DATA_DIR.mkdir(parents=True, exist_ok=True)
HLS_DIR.mkdir(parents=True, exist_ok=True)

DEFAULT_CHANNELS = [
    {
        "id": "suntv",
        "name": "Sun TV",
        "group": "Tamil",
        "mode": "relay",
        "source_url": os.getenv("SUNTV_SOURCE_URL", ""),
        "quality": "Smooth 25fps",
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


def get_media_info(msg: Message) -> Optional[dict]:
    media = msg.video or msg.document or msg.animation
    if not media:
        return None
    name = getattr(media, "file_name", None) or msg.caption or f"Movie_{msg.id}.mp4"
    mime = getattr(media, "mime_type", None) or "video/mp4"
    size = getattr(media, "file_size", None) or 0
    duration = getattr(media, "duration", None) or 0
    ok_ext = str(name).lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".webm", ".ts", ".m4v"))
    if not str(mime).startswith("video/") and not ok_ext and not msg.video:
        return None
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


def save_media_to_queue(item: dict) -> tuple[bool, int]:
    items = read_json(LIBRARY_FILE, [])
    for row in items:
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            return False, len(items)
    items.append(item)
    write_json(LIBRARY_FILE, items)
    return True, len(items)


@tg_client.on_message(filters.command("start"))
async def cmd_start(_, msg: Message):
    await msg.reply_text(
        "YPlay Ready.\n"
        "Send 1 or many movies/episodes directly (no command needed).\n"
        "Commands: /add (reply to file), /list, /del <number>"
    )


@tg_client.on_message(filters.command("list"))
async def cmd_list(_, msg: Message):
    items = read_json(LIBRARY_FILE, [])
    if not items:
        await msg.reply_text("YPlay list empty.")
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
    await msg.reply_text(f"Removed: {removed.get('title')}")


@tg_client.on_message(filters.command("add"))
async def cmd_add(_, msg: Message):
    target = msg.reply_to_message or msg
    info = get_media_info(target)
    if not info:
        await msg.reply_text("Reply to a video file with /add.")
        return
    added, total = save_media_to_queue(info)
    await msg.reply_text(f"{'Added' if added else 'Already in list'} (#{total}): {info['title']}")


@tg_client.on_message(filters.video | filters.document | filters.animation)
async def on_file(_, msg: Message):
    info = get_media_info(msg)
    if not info:
        return
    added, total = save_media_to_queue(info)
    if added:
        await msg.reply_text(f"Queued #{total}: {info['title']}")


async def start_suntv_relay():
    global ffmpeg_proc, last_error
    channels = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
    suntv = next((c for c in channels if c.get("id") == "suntv"), None)
    url = (suntv or {}).get("source_url") or os.getenv("SUNTV_SOURCE_URL", "")
    if not url:
        last_error = "Sun TV source URL not set yet"
        return False

    m3u8 = HLS_DIR / "index.m3u8"
    if ffmpeg_proc and ffmpeg_proc.poll() is None and m3u8.exists() and (time.time() - m3u8.stat().st_mtime) < 12:
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
        "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "3",
        "-i", url,
        "-vf", "fps=25,scale=-2:720,format=yuv420p",
        "-c:v", "libx264", "-preset", "veryfast", "-tune", "zerolatency",
        "-crf", "23", "-g", "50", "-keyint_min", "50", "-sc_threshold", "0",
        "-c:a", "aac", "-b:a", "128k", "-ac", "2",
        "-f", "hls", "-hls_time", "2", "-hls_list_size", "6",
        "-hls_flags", "delete_segments+append_list+independent_segments",
        str(m3u8),
    ]
    try:
        ffmpeg_proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    except Exception as e:
        last_error = f"FFmpeg start error: {e}"
        return False

    for _ in range(30):
        if m3u8.exists() and any(HLS_DIR.glob("*.ts")):
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
            channels = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
            suntv = next((c for c in channels if c.get("id") == "suntv"), None)
            if suntv and (suntv.get("source_url") or os.getenv("SUNTV_SOURCE_URL")):
                await start_suntv_relay()
        except Exception as e:
            global last_error
            last_error = str(e)
        await asyncio.sleep(5)


@app.on_event("startup")
async def startup():
    await tg_client.start()
    asyncio.create_task(relay_watcher())


@app.on_event("shutdown")
async def shutdown():
    if ffmpeg_proc and ffmpeg_proc.poll() is None:
        ffmpeg_proc.terminate()
    try:
        await tg_client.stop()
    except Exception:
        pass


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "bot_connected": tg_client.is_connected,
        "yplay_count": len(read_json(LIBRARY_FILE, [])),
        "suntv_active": ffmpeg_proc is not None and ffmpeg_proc.poll() is None,
        "suntv_error": last_error,
    }


@app.get("/api/yplay/library")
async def yplay_library():
    items = read_json(LIBRARY_FILE, [])
    return {"count": len(items), "items": items}


@app.get("/api/yplay/stream/{index}")
async def yplay_stream(index: int, request: Request, range_header: Optional[str] = Header(default=None)):
    items = read_json(LIBRARY_FILE, [])
    if index < 0 or index >= len(items):
        raise HTTPException(status_code=404, detail="Movie not found in YPlay queue")

    item = items[index]
    msg = await tg_client.get_messages(int(item["chat_id"]), int(item["message_id"]))
    media = msg.video or msg.document or msg.animation
    if not media:
        raise HTTPException(status_code=404, detail="Media missing in Telegram")

    total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
    mime = item.get("mime_type") or getattr(media, "mime_type", None) or "video/mp4"

    start_byte = 0
    end_byte = total_size - 1 if total_size > 0 else None

    if range_header and total_size > 0:
        try:
            _, rng = range_header.strip().split("=", 1)
            s_part, e_part = rng.split("-", 1)
            if s_part:
                start_byte = max(0, int(s_part))
            if e_part:
                end_byte = min(total_size - 1, int(e_part))
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid range")

    chunk_size = 1024 * 1024
    offset_chunks = start_byte // chunk_size
    trim_first = start_byte % chunk_size

    async def stream_gen():
        sent = 0
        limit = (end_byte - start_byte + 1) if (total_size > 0 and end_byte is not None) else None
        first = True
        async for chunk in tg_client.stream_media(msg, offset=offset_chunks):
            if await request.is_disconnected():
                break
            if first and trim_first > 0:
                chunk = chunk[trim_first:]
                first = False
            if not chunk:
                continue
            if limit is not None:
                rem = limit - sent
                if rem <= 0:
                    break
                if len(chunk) > rem:
                    chunk = chunk[:rem]
            sent += len(chunk)
            yield chunk

    headers = {"Accept-Ranges": "bytes", "Content-Type": mime, "Cache-Control": "no-cache"}
    if total_size > 0 and end_byte is not None and range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        headers["Content-Length"] = str(end_byte - start_byte + 1)
        return StreamingResponse(stream_gen(), status_code=206, headers=headers, media_type=mime)

    if total_size > 0:
        headers["Content-Length"] = str(total_size)
    return StreamingResponse(stream_gen(), status_code=200, headers=headers, media_type=mime)


@app.post("/api/channels/suntv")
async def set_suntv(request: Request):
    body = await request.json()
    url = (body.get("source_url") or "").strip()
    if not url:
        raise HTTPException(status_code=400, detail="source_url required")
    channels = read_json(CHANNELS_FILE, DEFAULT_CHANNELS)
    for ch in channels:
        if ch.get("id") == "suntv":
            ch["source_url"] = url
    write_json(CHANNELS_FILE, channels)
    ok = await start_suntv_relay()
    return {"ok": ok, "error": last_error}


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
