import json
import os
import re
import subprocess
import time
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pyrogram import Client, filters
from pyrogram.types import Message

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
API_ID = int(os.getenv("TELEGRAM_API_ID", "6"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "eb06d4abfb49dc3eeb1aeb98ae0f581e")

if not BOT_TOKEN:
    raise RuntimeError("TELEGRAM_BOT_TOKEN missing")

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
LIBRARY_FILE = DATA_DIR / "yplay_library.json"
SESSION_FILE = DATA_DIR / "ystream_bot"
DATA_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 1024 * 1024  # 1 MiB Telegram chunk

app = FastAPI(title="YStream Direct")
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

msg_cache: dict[str, Message] = {}
detected_tunnel_url: str = ""


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


def update_detected_tunnel(request: Request):
    global detected_tunnel_url
    for hdr in (request.headers.get("x-forwarded-host", ""), request.headers.get("host", "")):
        host = hdr.split(",")[0].strip().split(":")[0]
        if host.endswith(".trycloudflare.com"):
            detected_tunnel_url = f"https://{host}"
            return


def get_active_tunnel_url() -> str:
    env_url = os.getenv("TUNNEL_URL", "").strip().rstrip("/")
    if env_url.startswith("https://"):
        return env_url
    if detected_tunnel_url:
        return detected_tunnel_url
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


def safe_ascii_filename(name: str) -> str:
    clean = re.sub(r"[^a-zA-Z0-9._-]", "_", name or "video.mp4").strip("_")
    if not clean:
        clean = "video.mp4"
    if "." not in clean:
        clean += ".mp4"
    return clean[:80]


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


def save_media_to_queue(item: dict) -> tuple[bool, int]:
    items = read_json(LIBRARY_FILE, [])
    for idx, row in enumerate(items):
        if row.get("chat_id") == item["chat_id"] and row.get("message_id") == item["message_id"]:
            return False, idx + 1
    items.append(item)
    write_json(LIBRARY_FILE, items)
    return True, len(items)


@tg_client.on_message(filters.command("start"))
async def cmd_start(_, msg: Message):
    await msg.reply_text("YPlay Ready. Forward any movie/video to queue it.")


@tg_client.on_message(filters.command("list"))
async def cmd_list(_, msg: Message):
    items = read_json(LIBRARY_FILE, [])
    if not items:
        await msg.reply_text("Queue is empty.")
        return
    lines = [f"{i + 1}. {x.get('title')}" for i, x in enumerate(items)]
    await msg.reply_text("Queue:\n" + "\n".join(lines[-25:]))


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
    await msg.reply_text(f"Removed: {removed.get('title')}")


@tg_client.on_message(filters.command("add"))
async def cmd_add(_, msg: Message):
    target = msg.reply_to_message or msg
    info = get_media_info(target)
    if not info:
        await msg.reply_text("Reply to a video file with /add.")
        return
    added, num = save_media_to_queue(info)
    await msg.reply_text(f"{'Added' if added else 'Exists'} #{num}: {info['title']}")


@tg_client.on_message(filters.video | filters.document | filters.animation)
async def on_file(_, msg: Message):
    info = get_media_info(msg)
    if not info:
        return
    added, num = save_media_to_queue(info)
    if added:
        await msg.reply_text(f"Queued #{num}: {info['title']}")


@app.on_event("startup")
async def startup():
    await tg_client.start()


@app.on_event("shutdown")
async def shutdown():
    try:
        await tg_client.stop()
    except Exception:
        pass


@app.get("/health")
async def health(request: Request):
    update_detected_tunnel(request)
    items = read_json(LIBRARY_FILE, [])
    return {
        "status": "ok",
        "bot_connected": tg_client.is_connected,
        "yplay_count": len(items),
        "tunnel_url": get_active_tunnel_url(),
    }


@app.get("/api/yplay/library")
async def yplay_library(request: Request):
    update_detected_tunnel(request)
    items = read_json(LIBRARY_FILE, [])
    enriched = []
    for idx, x in enumerate(items):
        row = dict(x)
        row["index"] = idx
        row["filename"] = safe_ascii_filename(x.get("title", "video.mp4"))
        enriched.append(row)
    return {"count": len(enriched), "items": enriched}


@app.get("/dl/{index}/{filename}")
@app.get("/api/yplay/stream/{index}")
async def yplay_stream(index: int, request: Request, filename: str = "video.mp4"):
    update_detected_tunnel(request)
    items = read_json(LIBRARY_FILE, [])
    if index < 0 or index >= len(items):
        raise HTTPException(status_code=404, detail="Movie not found")

    item = items[index]
    msg = await get_tg_message(item)
    media = msg.video or msg.document or msg.animation
    total_size = int(getattr(media, "file_size", 0) or item.get("file_size") or 0)
    mime = item.get("mime_type") or getattr(media, "mime_type", None) or "video/mp4"
    if mime == "application/octet-stream":
        mime = "video/mp4"

    if total_size <= 0:
        raise HTTPException(status_code=500, detail="Unknown media size")

    range_header = request.headers.get("range")
    start_byte = 0
    end_byte = total_size - 1

    if range_header and "=" in range_header:
        try:
            _, rng = range_header.strip().split("=", 1)
            s_part, e_part = rng.split("-", 1)
            if not s_part and e_part:
                start_byte = max(0, total_size - int(e_part))
                end_byte = total_size - 1
            else:
                if s_part:
                    start_byte = max(0, int(s_part))
                if e_part:
                    end_byte = min(total_size - 1, int(e_part))
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid range")

    if start_byte > end_byte or start_byte >= total_size:
        raise HTTPException(status_code=416, detail="Invalid range bounds")

    length = end_byte - start_byte + 1
    offset_chunks = start_byte // CHUNK_SIZE
    trim_first = start_byte % CHUNK_SIZE
    dl_name = safe_ascii_filename(item.get("title", filename))

    async def direct_stream():
        for attempt in range(2):
            sent = 0
            first = True
            active_msg = msg if attempt == 0 else await get_tg_message(item, force_refresh=True)
            try:
                async for chunk in tg_client.stream_media(active_msg, offset=offset_chunks):
                    if await request.is_disconnected():
                        return
                    if first:
                        first = False
                        if trim_first > 0:
                            chunk = chunk[trim_first:]
                    if not chunk:
                        continue
                    rem = length - sent
                    if rem <= 0:
                        return
                    if len(chunk) > rem:
                        chunk = chunk[:rem]
                    sent += len(chunk)
                    yield chunk
                    if sent >= length:
                        return
                return
            except Exception:
                msg_cache.pop(item["id"], None)
                if sent > 0 or attempt == 1:
                    return

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Length": str(length),
        "Content-Disposition": f'inline; filename="{dl_name}"',
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        return StreamingResponse(direct_stream(), status_code=206, headers=headers, media_type=mime)

    return StreamingResponse(direct_stream(), status_code=200, headers=headers, media_type=mime)
