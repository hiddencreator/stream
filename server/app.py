import asyncio
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

CHUNK_SIZE = 1024 * 1024        # 1 MiB block
AHEAD_BLOCKS = 24               # Buffer 24 MB ahead in RAM (YouTube-style)
BEHIND_BLOCKS = 8               # Keep 8 MB behind in RAM for instant -10s rewind
HTTP_WINDOW_BYTES = 8 * CHUNK_SIZE  # 8 MB max per HTTP 206 response

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


class MovieBuffer:
    def __init__(self, item_id: str, total_size: int):
        self.item_id = item_id
        self.total_size = total_size
        self.total_blocks = (total_size + CHUNK_SIZE - 1) // CHUNK_SIZE
        self.blocks: dict[int, bytes] = {}
        self.pinned: dict[int, bytes] = {}
        self.events: dict[int, asyncio.Event] = {}
        self.reader_task: Optional[asyncio.Task] = None
        self.reader_start: int = -1
        self.reader_end: int = -1
        self.playhead: int = 0
        self.tail_lock = asyncio.Lock()
        self.last_used = time.monotonic()


movie_buffers: dict[str, MovieBuffer] = {}


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


def get_or_create_buffer(item_id: str, total_size: int) -> MovieBuffer:
    # Keep only active movie in RAM to stay lightweight
    for k in list(movie_buffers.keys()):
        if k != item_id:
            old = movie_buffers.pop(k)
            if old.reader_task and not old.reader_task.done():
                old.reader_task.cancel()
            old.blocks.clear()
            old.pinned.clear()

    mb = movie_buffers.get(item_id)
    if mb is None or mb.total_size != total_size:
        mb = MovieBuffer(item_id, total_size)
        movie_buffers[item_id] = mb
    mb.last_used = time.monotonic()
    return mb


def prune_old_blocks(mb: MovieBuffer):
    min_keep = max(0, mb.playhead - BEHIND_BLOCKS)
    for b in list(mb.blocks.keys()):
        if b < min_keep:
            mb.blocks.pop(b, None)
            mb.events.pop(b, None)


def start_reader(mb: MovieBuffer, item: dict, start_b: int, target_end_b: int):
    if mb.reader_task and not mb.reader_task.done():
        mb.reader_task.cancel()

    mb.reader_start = start_b
    mb.reader_end = min(mb.total_blocks - 1, target_end_b)

    async def _run():
        curr_b = start_b
        buf = bytearray()
        try:
            for attempt in range(2):
                msg = await get_tg_message(item, force_refresh=(attempt == 1))
                try:
                    async for chunk in tg_client.stream_media(msg, offset=curr_b):
                        buf.extend(chunk)
                        while len(buf) >= CHUNK_SIZE:
                            blk = bytes(buf[:CHUNK_SIZE])
                            del buf[:CHUNK_SIZE]
                            mb.blocks[curr_b] = blk
                            if curr_b <= 1:
                                mb.pinned[curr_b] = blk
                            ev = mb.events.get(curr_b)
                            if ev:
                                ev.set()
                            curr_b += 1
                            if curr_b > mb.reader_end or curr_b >= mb.total_blocks:
                                prune_old_blocks(mb)
                                return
                        if curr_b in mb.blocks:
                            # Skip ahead if already buffered
                            while curr_b in mb.blocks and curr_b <= mb.reader_end:
                                curr_b += 1
                            if curr_b > mb.reader_end:
                                return
                            break
                    # Flush final partial EOF block
                    if buf and curr_b < mb.total_blocks:
                        blk = bytes(buf)
                        buf.clear()
                        mb.blocks[curr_b] = blk
                        ev = mb.events.get(curr_b)
                        if ev:
                            ev.set()
                        curr_b += 1
                    if curr_b > mb.reader_end or curr_b >= mb.total_blocks:
                        return
                except asyncio.CancelledError:
                    raise
                except Exception:
                    msg_cache.pop(item["id"], None)
                    if attempt == 1:
                        return
        except asyncio.CancelledError:
            pass
        finally:
            # Release any waiting callers on abort
            for b in range(curr_b, mb.reader_end + 1):
                ev = mb.events.get(b)
                if ev:
                    ev.set()

    mb.reader_task = asyncio.create_task(_run())


def ensure_ahead_buffer(mb: MovieBuffer, item: dict, b_idx: int):
    mb.playhead = b_idx
    mb.last_used = time.monotonic()
    prune_old_blocks(mb)

    # Check if next 12 blocks are missing; if so, trigger/extend reader up to +24 blocks
    look_limit = min(mb.total_blocks - 1, b_idx + 12)
    first_missing = None
    for b in range(b_idx, look_limit + 1):
        if b not in mb.blocks and b not in mb.pinned:
            first_missing = b
            break

    if first_missing is None:
        return

    reader_active = mb.reader_task is not None and not mb.reader_task.done()
    if reader_active and mb.reader_start <= first_missing <= mb.reader_end:
        # Extend target end smoothly so reader doesn't stop early
        mb.reader_end = min(mb.total_blocks - 1, b_idx + AHEAD_BLOCKS)
        return

    start_reader(mb, item, first_missing, b_idx + AHEAD_BLOCKS)


async def get_block(mb: MovieBuffer, item: dict, b_idx: int) -> bytes:
    if b_idx < 0 or b_idx >= mb.total_blocks:
        return b""
    if b_idx in mb.blocks:
        ensure_ahead_buffer(mb, item, b_idx)
        return mb.blocks[b_idx]
    if b_idx in mb.pinned:
        return mb.pinned[b_idx]

    # Tail metadata probe (last 3 blocks for MP4 moov atom) -> fetch independently without cancelling forward playback
    if b_idx >= mb.total_blocks - 3 and mb.playhead < mb.total_blocks - 8:
        async with mb.tail_lock:
            if b_idx in mb.pinned:
                return mb.pinned[b_idx]
            for attempt in range(2):
                msg = await get_tg_message(item, force_refresh=(attempt == 1))
                try:
                    data = bytearray()
                    async for chunk in tg_client.stream_media(msg, offset=b_idx):
                        data.extend(chunk)
                        if len(data) >= CHUNK_SIZE:
                            break
                    blk = bytes(data[:CHUNK_SIZE]) if len(data) >= CHUNK_SIZE else bytes(data)
                    if blk:
                        mb.pinned[b_idx] = blk
                        return blk
                except Exception:
                    msg_cache.pop(item["id"], None)
        return b""

    ensure_ahead_buffer(mb, item, b_idx)
    if b_idx in mb.blocks:
        return mb.blocks[b_idx]

    ev = mb.events.get(b_idx)
    if ev is None:
        ev = asyncio.Event()
        mb.events[b_idx] = ev

    try:
        await asyncio.wait_for(ev.wait(), timeout=20.0)
    except asyncio.TimeoutError:
        return b""
    return mb.blocks.get(b_idx) or mb.pinned.get(b_idx) or b""


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
    await msg.reply_text("YPlay Ready. Forward any movie/video to stream instantly.")


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
    old = movie_buffers.pop(removed["id"], None)
    if old and old.reader_task and not old.reader_task.done():
        old.reader_task.cancel()
    await msg.reply_text(f"Removed: {removed.get('title')}")


@tg_client.on_message(filters.command("add"))
async def cmd_add(_, msg: Message):
    target = msg.reply_to_message or msg
    info = get_media_info(target)
    if not info:
        await msg.reply_text("Reply to a video file with /add.")
        return
    added, num = save_media_to_queue(info)
    tunnel = get_active_tunnel_url()
    fname = safe_ascii_filename(info["title"])
    link_txt = f"\nDirect Link:\n{tunnel}/dl/{num - 1}/{fname}" if tunnel else ""
    await msg.reply_text(f"{'Added' if added else 'Exists'} #{num}: {info['title']}{link_txt}")


@tg_client.on_message(filters.video | filters.document | filters.animation)
async def on_file(_, msg: Message):
    info = get_media_info(msg)
    if not info:
        return
    added, num = save_media_to_queue(info)
    tunnel = get_active_tunnel_url()
    fname = safe_ascii_filename(info["title"])
    link_txt = f"\nDirect Link:\n{tunnel}/dl/{num - 1}/{fname}" if tunnel else ""
    if added:
        await msg.reply_text(f"Direct Link Ready #{num}: {info['title']}{link_txt}")


@app.on_event("startup")
async def startup():
    await tg_client.start()


@app.on_event("shutdown")
async def shutdown():
    for mb in movie_buffers.values():
        if mb.reader_task and not mb.reader_task.done():
            mb.reader_task.cancel()
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
            if s_part:
                start_byte = max(0, int(s_part))
            if e_part:
                end_byte = min(total_size - 1, int(e_part))
            else:
                end_byte = min(total_size - 1, start_byte + HTTP_WINDOW_BYTES - 1)
        except Exception:
            raise HTTPException(status_code=416, detail="Invalid range")
        # Cap each 206 slice to 8 MB so Cloudflare Tunnel never drops long sockets
        end_byte = min(end_byte, start_byte + HTTP_WINDOW_BYTES - 1)

    if start_byte > end_byte or start_byte >= total_size:
        raise HTTPException(status_code=416, detail="Invalid range bounds")

    length = end_byte - start_byte + 1
    mb = get_or_create_buffer(item["id"], total_size)
    dl_name = safe_ascii_filename(item.get("title", filename))

    async def buffered_stream():
        pos = start_byte
        while pos <= end_byte:
            if await request.is_disconnected():
                break
            b_idx = pos // CHUNK_SIZE
            b_off = pos % CHUNK_SIZE
            raw = await get_block(mb, item, b_idx)
            if not raw or b_off >= len(raw):
                break
            take = min(len(raw) - b_off, end_byte - pos + 1)
            yield raw[b_off : b_off + take]
            pos += take

    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": mime,
        "Content-Length": str(length),
        "Content-Disposition": f'inline; filename="{dl_name}"',
    }
    if range_header:
        headers["Content-Range"] = f"bytes {start_byte}-{end_byte}/{total_size}"
        return StreamingResponse(buffered_stream(), status_code=206, headers=headers, media_type=mime)

    return StreamingResponse(buffered_stream(), status_code=200, headers=headers, media_type=mime)
