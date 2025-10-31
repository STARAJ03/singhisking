# --- BEGIN keepalive server for Koyeb ---
import threading
from flask import Flask

def start_keepalive_server():
    from waitress import serve
    app = Flask(__name__)

    @app.route('/')
    def home():
        return "✅ Bot is alive", 200

    serve(app, host="0.0.0.0", port=8000)

# Run Flask web server in a background thread
threading.Thread(target=start_keepalive_server, daemon=True).start()
# --- END keepalive server ---
import os
import re
import json
import logging
import asyncio
import time
import shutil
import asyncio

from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError, BadMsgNotification
from pyrogram.types import Message
from typing import Dict, List, Optional

# NEW imports for async HTTP downloads and youtube support
import aiohttp
import aiofiles
from yt_dlp import YoutubeDL
from concurrent.futures import ThreadPoolExecutor
from motor.motor_asyncio import AsyncIOMotorClient
from bson.binary import Binary
from bson.objectid import ObjectId
from datetime import datetime, timezone

# ─── Logging Setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s │ %(name)s │ %(levelname)s │ %(message)s"
)
logger = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────────
API_ID = 27765349
API_HASH = "9df1f705c8047ac0d723b29069a1332b"
BOT_TOKEN = os.getenv("BOT_TOKEN")
MONGODB_URI = os.getenv("MONGODB_URI", "")
CHANNEL_USERNAME = os.getenv("CHANNEL_USERNAME", "").strip()  # Optional: @publicgroupname
LOG_CHANNEL_ID_RAW = os.getenv("LOG_CHANNEL_ID", "").strip()  # Optional: -100... or @channelusername
try:
    LOG_CHANNEL_ID: Optional[int | str] = int(LOG_CHANNEL_ID_RAW) if LOG_CHANNEL_ID_RAW else None
except ValueError:
    LOG_CHANNEL_ID = LOG_CHANNEL_ID_RAW if LOG_CHANNEL_ID_RAW else None

# Only these user IDs can trigger /setchannel or upload
ALLOWED_USER_IDS = [1116405290]

app = Client(
    "simple_subject_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    workdir="./",
    sleep_threshold=60  # handle flood waits automatically
)

# ThreadPool for blocking tasks (yt-dlp)
_thread_pool = ThreadPoolExecutor(max_workers=2)

# ─── Global State ───────────────────────────────────────────────────────────────
active_downloads: Dict[int, bool] = {}
user_data: Dict[int, dict] = {}
_job_worker_running = False
_job_worker_task = None

# Persistent cache file for forum topics mapping: { chat_id: { subject_norm: thread_id } }
FORUM_CACHE_PATH = "forum_threads.json"

def _normalize_subject(s: str) -> str:
    return s.strip().casefold()

def load_forum_cache() -> Dict[str, Dict[str, int]]:
    try:
        if os.path.exists(FORUM_CACHE_PATH):
            with open(FORUM_CACHE_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                return data if isinstance(data, dict) else {}
    except Exception as e:
        logger.warning(f"Could not load forum cache: {e}")
    return {}

def save_forum_cache(cache: Dict[str, Dict[str, int]]) -> None:
    try:
        with open(FORUM_CACHE_PATH, "w", encoding="utf-8") as f:
            json.dump(cache, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"Could not save forum cache: {e}")

# Mongo persistence (optional)
_mongo_client: Optional[AsyncIOMotorClient] = None
_mongo_col = None

async def get_mongo_collection():
    global _mongo_client, _mongo_col
    if not MONGODB_URI:
        return None

async def get_jobs_collection():
    try:
        col = await get_mongo_collection()
        if col is None:
            return None
        db = col.database
        return db["upload_jobs"]
    except Exception as e:
        logger.warning(f"Jobs collection unavailable: {e}")
        return None

async def enqueue_job(user_id: int, channel_id: int, lines: List[str], start_number: int, batch_name: str, downloaded_by: str) -> Optional[str]:
    col = await get_jobs_collection()
    if col is None:
        return None
    doc = {
        "user_id": user_id,
        "channel_id": int(channel_id),
        "lines": lines,
        "start_number": int(start_number),
        "batch_name": batch_name,
        "downloaded_by": downloaded_by,
        "total": len(lines),
        "status": "pending",
        "created_at": int(time.time()),
        "updated_at": int(time.time()),
    }
    res = await col.insert_one(doc)
    return str(res.inserted_id)

async def fetch_next_pending_job() -> Optional[dict]:
    col = await get_jobs_collection()
    if col is None:
        return None
    job = await col.find_one({"status": "pending"}, sort=[("created_at", 1)])
    return job

async def set_job_status(job_id: str, status: str) -> None:
    col = await get_jobs_collection()
    if col is None:
        return
    try:
        await col.update_one({"_id": ObjectId(job_id)}, {"$set": {"status": status, "updated_at": int(time.time())}})
    except Exception:
        pass

async def delete_pending_job(job_id: str) -> bool:
    col = await get_jobs_collection()
    if col is None:
        return False
    try:
        res = await col.delete_one({"_id": ObjectId(job_id), "status": "pending"})
        return res.deleted_count == 1
    except Exception:
        return False

async def list_jobs(limit: int = 10) -> List[dict]:
    col = await get_jobs_collection()
    if col is None:
        return []
    cursor = col.find({}, sort=[("created_at", 1)], limit=limit)
    return [doc async for doc in cursor]

async def _run_job_worker(client: Client):
    global _job_worker_running
    if _job_worker_running:
        return
    _job_worker_running = True
    try:
        while True:
            job = await fetch_next_pending_job()
            if not job:
                _job_worker_running = False
                return
            job_id = str(job.get("_id"))
            await set_job_status(job_id, "running")
            try:
                # Build user_data and status message in user's DM
                uid = int(job["user_id"])
                lines = job["lines"]
                start_idx = int(job["start_number"])
                batch_name = job["batch_name"]
                channel_id = int(job["channel_id"])
                downloaded_by = job["downloaded_by"]
                user_data[uid] = {
                    "lines": lines,
                    "start_number": start_idx,
                    "batch_name": batch_name,
                    "channel_id": channel_id,
                    "downloaded_by": downloaded_by,
                    "total": len(lines),
                }
                try:
                    dm = await client.send_message(chat_id=uid, text=f"🚀 Starting queued upload to {channel_id} (job {job_id[:6]}).")
                except Exception:
                    # Fallback: create a dummy message object with minimal interface
                    dm = None
                # Call existing processing using the prepared user_data
                await start_processing(client, dm or Message(id=0), uid)
            except Exception as e:
                logger.exception(f"Job {job_id} failed: {e}")
                await set_job_status(job_id, "failed")
            else:
                await set_job_status(job_id, "completed")
    finally:
        _job_worker_running = False
    try:
        if _mongo_col is not None:
            return _mongo_col
        _mongo_client = AsyncIOMotorClient(MONGODB_URI, serverSelectionTimeoutMS=3000)
        # Use default DB if provided in URI; else fallback to 'app'
        try:
            db = _mongo_client.get_default_database()  # may raise if none
        except Exception:
            db = _mongo_client["app"]
        _mongo_col = db["forum_threads"]
        # Force a quick ping to validate connection
        await _mongo_client.admin.command("ping")
        logger.info("MongoDB connected for forum thread mapping")
        return _mongo_col
    except Exception as e:
        logger.warning(f"MongoDB unavailable, falling back to local cache: {e}")
        _mongo_col = None
        return None

async def mongo_get_thread_id(chat_id: int, subject_norm: str) -> Optional[int]:
    col = await get_mongo_collection()
    if col is None:
        return None
    doc = await col.find_one({"chat_id": chat_id, "subject_norm": subject_norm})
    return int(doc["thread_id"]) if doc and "thread_id" in doc else None

async def mongo_set_thread_id(chat_id: int, subject_norm: str, thread_id: int) -> None:
    col = await get_mongo_collection()
    if col is None:
        return
    await col.update_one(
        {"chat_id": chat_id, "subject_norm": subject_norm},
        {"$set": {"thread_id": int(thread_id), "updated_at": int(time.time())}},
        upsert=True,
    )

@app.on_message(filters.command("ping") & filters.private)
async def ping_handler(client: Client, message: Message):
    await message.reply_text("pong")

@app.on_message(filters.command("whoami") & filters.private)
async def whoami_handler(client: Client, message: Message):
    me = await client.get_me()
    logger.info(f"Bot logged in as {getattr(me, 'username', None)} ({me.id})")
    await message.reply_text(f"@{getattr(me, 'username', 'unknown')} ({me.id})")

async def get_state_collection():
    """Return a collection for storing small bot state blobs (e.g., session backup)."""
    try:
        col = await get_mongo_collection()
        if col is None:
            return None
        # Use same DB as forum_threads but a different collection
        # We can derive DB from _mongo_col
        db = col.database
        return db["bot_state"]
    except Exception as e:
        logger.warning(f"State collection unavailable: {e}")
        return None

async def load_session_from_mongo(session_path: str = "simple_subject_bot.session") -> None:
    col = await get_state_collection()
    if col is None:
        return
    try:
        doc = await col.find_one({"_id": "pyrogram_session"})
        if doc and doc.get("bytes"):
            data = doc["bytes"]
            try:
                # Only restore if missing to avoid overwriting newer local state
                if not os.path.exists(session_path):
                    with open(session_path, "wb") as f:
                        f.write(bytes(data))
                    logger.info("Restored Pyrogram session from MongoDB backup")
            except Exception as e:
                logger.warning(f"Could not write session file: {e}")
    except Exception as e:
        logger.warning(f"Session restore from Mongo failed: {e}")

async def save_session_to_mongo(session_path: str = "simple_subject_bot.session") -> None:
    col = await get_state_collection()
    if col is None:
        return
    if not os.path.exists(session_path):
        return
    try:
        with open(session_path, "rb") as f:
            data = f.read()
        await col.update_one(
            {"_id": "pyrogram_session"},
            {"$set": {"bytes": Binary(data), "updated_at": int(time.time())}},
            upsert=True,
        )
        logger.info("Backed up Pyrogram session to MongoDB")
    except Exception as e:
        logger.warning(f"Session backup to Mongo failed: {e}")

@app.on_message(filters.command("warm") & filters.private)
async def warm_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in ALLOWED_USER_IDS:
        return
    args = message.text.strip().split(maxsplit=1)
    if len(args) < 2:
        await message.reply_text("Usage: /warm <chat_id>")
        return
    try:
        target = args[1]
        # Try to parse as int, else keep as string
        try:
            target_id = int(target)
        except ValueError:
            target_id = target
        chat = await client.get_chat(target_id)
        # Access an attribute to force store; Pyrogram session will persist it on disk
        _ = getattr(chat, "id", None)
        logger.info(f"Warm success for chat {target_id}: id={getattr(chat, 'id', None)}")
        # Save session backup so peer persists across redeploys
        try:
            await save_session_to_mongo()
        except Exception:
            pass
        await message.reply_text(f"✅ Warmed peer for chat: {getattr(chat, 'id', target_id)}")
    except Exception as e:
        logger.error(f"Warm failed for {args[1]}: {e}")
        await message.reply_text(f"❌ Warm failed: {e}")

# ─── Helper Functions ───────────────────────────────────────────────────────────

async def duration_async(filename: str) -> float:
    """Get video duration using ffprobe."""
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe',
            '-v', 'error',
            '-show_entries', 'format=duration',
            '-of', 'default=noprint_wrappers=1:nokey=1',
            filename,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return float(stdout.decode().strip())
        else:
            return 0.0
    except Exception as e:
        logger.error(f"Duration error: {e}")
        return 0.0

async def extract_thumbnail_async(filename: str, timestamp: str = "00:00:10") -> Optional[str]:
    """Generate a thumbnail from the video at the given timestamp."""
    thumbnail_path = f"{filename}.jpg"
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg',
            '-i', filename,
            '-ss', timestamp,
            '-vframes', '1',
            '-y', thumbnail_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        return thumbnail_path if os.path.exists(thumbnail_path) else None
    except Exception as e:
        logger.error(f"Thumbnail error: {e}")
        return None

def extract_subjects(title: str) -> List[str]:
    """
    Extract all "[Subject]" tags from the title string.
    If none are found, returns ["General"].
    """
    subjects = list(set(re.findall(r'\[([^\]]+)\]', title)))
    return subjects if subjects else ["General"]

def clean_title(title: str) -> str:
    """Sanitize title for use as a filename (remove forbidden characters)."""
    return re.sub(r'[^\w\-_. ]', "", title.strip())

def build_caption(subject: str, index_number: int, title: str, batch: str, downloaded_by: str, link: Optional[str] = None) -> str:
    subject_text = subject.strip().upper()
    lines = [
        f"✧ {subject_text} ✧",
        "━━━━━━━━━━",
        f"▸ 𝙄𝙣𝙙𝙚𝙭  -  {index_number}",
        f"▸ 𝙏𝙞𝙩𝙡𝙚    -  {title.strip()}",
        "━━━━━━━━━━",
        batch.strip(),
        "━━━━━━━━━━",
        f"▸ 𝙀𝙭𝙩𝙧𝙖𝙘𝙩𝙚𝙙 𝘽𝙮 - {downloaded_by.strip()}"
    ]
    if link:
        lines.insert(5, f"▸ 𝙇𝙞𝙣𝙠   -  {link.strip()}")
    return "\n".join(lines)

def select_pdf_filename(title_with_subject: str) -> str:
    """
    From a title like "[SUBJECT]Batch | Core Title | Teacher", extract the middle pipe segment as the filename.
    Fallback to the cleaned full title if the expected pattern is not present.
    """
    # remove [Subject] prefix if present
    title_no_subject = re.sub(r"\[[^\]]+\]\s*", "", title_with_subject).strip()
    parts = [p.strip() for p in title_no_subject.split("|") if p.strip()]
    if len(parts) >= 2:
        core = parts[1]  # middle segment
        return clean_title(core)
    return clean_title(title_no_subject)

# --- Bot API helpers for forum topics (works even if Pyrogram runtime lacks message_thread_id) ---
BOT_API_BASE = None

def _bot_api_base() -> str:
    global BOT_API_BASE
    base = f"https://api.telegram.org/bot{BOT_TOKEN}"
    return base

async def bot_api_create_forum_topic(chat_id: int, title: str) -> Optional[int]:
    url = _bot_api_base() + "/createForumTopic"
    payload = {"chat_id": chat_id, "name": title}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI createForumTopic failed: {data}")
            return data.get("result", {}).get("message_thread_id")

async def bot_api_send_message(chat_id: int, thread_id: int, text: str) -> None:
    url = _bot_api_base() + "/sendMessage"
    payload = {"chat_id": chat_id, "message_thread_id": thread_id, "text": text, "disable_web_page_preview": False}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendMessage failed: {data}")

async def bot_api_send_document(chat_id: int, thread_id: int, file_path: str, caption: str) -> None:
    url = _bot_api_base() + "/sendDocument"
    form = aiohttp.FormData()
    form.add_field("chat_id", str(chat_id))
    form.add_field("message_thread_id", str(thread_id))
    form.add_field("caption", caption)
    form.add_field("document", open(file_path, "rb"), filename=os.path.basename(file_path))
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form, timeout=300) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendDocument failed: {data}")

async def bot_api_send_document_by_id(chat_id: int, thread_id: int, file_id: str, caption: str) -> None:
    url = _bot_api_base() + "/sendDocument"
    payload = {
        "chat_id": chat_id,
        "message_thread_id": thread_id,
        "document": file_id,
        "caption": caption,
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=300) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendDocument (by id) failed: {data}")

async def bot_api_send_video(chat_id: int, thread_id: int, file_path: str, caption: str, duration: Optional[int] = None, thumb_path: Optional[str] = None) -> None:
    url = _bot_api_base() + "/sendVideo"
    form = aiohttp.FormData()
    form.add_field("chat_id", str(chat_id))
    form.add_field("message_thread_id", str(thread_id))
    form.add_field("caption", caption)
    if duration is not None:
        form.add_field("duration", str(int(duration)))
    # Hint player for progressive playback
    form.add_field("supports_streaming", "true")
    form.add_field("video", open(file_path, "rb"), filename=os.path.basename(file_path))
    if thumb_path and os.path.exists(thumb_path):
        form.add_field("thumbnail", open(thumb_path, "rb"), filename=os.path.basename(thumb_path))
    async with aiohttp.ClientSession() as session:
        async with session.post(url, data=form, timeout=600) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendVideo failed: {data}")

async def bot_api_send_video_by_id(chat_id: int, thread_id: int, file_id: str, caption: str, duration: Optional[int] = None) -> None:
    url = _bot_api_base() + "/sendVideo"
    payload = {
        "chat_id": chat_id,
        "message_thread_id": thread_id,
        "video": file_id,
        "caption": caption,
    }
    if duration is not None:
        payload["duration"] = int(duration)
    # Ensure Telegram treats it as streamable in the UI
    payload["supports_streaming"] = True
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=300) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendVideo (by id) failed: {data}")

async def bot_api_get_chat(chat_id: int) -> dict:
    url = _bot_api_base() + "/getChat"
    params = {"chat_id": chat_id}
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI getChat failed: {data}")
            return data.get("result", {})

async def bot_api_pin_message(chat_id: int, message_id: int) -> None:
    url = _bot_api_base() + "/pinChatMessage"
    payload = {"chat_id": chat_id, "message_id": message_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI pinChatMessage failed: {data}")

async def bot_api_delete_message(chat_id: int, message_id: int) -> None:
    url = _bot_api_base() + "/deleteMessage"
    payload = {"chat_id": chat_id, "message_id": message_id}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI deleteMessage failed: {data}")

# --- NEW: http download helper (async) ---
async def _download_http_to_file(session: aiohttp.ClientSession, url: str, tmp_path: str) -> None:
    """
    Streams a resource from `url` and writes to tmp_path atomically.
    Raises exceptions on HTTP errors.
    """
    CHUNK = 64 * 1024
    async with session.get(url, timeout=aiohttp.ClientTimeout(total=600)) as resp:
        if resp.status != 200:
            raise Exception(f"HTTP {resp.status} for {url}")
        # write to temp file then move
        tmp_write = tmp_path + ".part"
        async with aiofiles.open(tmp_write, "wb") as f:
            async for chunk in resp.content.iter_chunked(CHUNK):
                await f.write(chunk)
        # atomic move
        shutil.move(tmp_write, tmp_path)

# --- NEW: yt-dlp wrapper (runs in thread) ---
def _ydl_download_blocking(url: str, out_template: str) -> str:
    """
    Blocking function to download via yt-dlp. Returns output file path.
    """
    opts = {
        "outtmpl": out_template,
        "format": "best[ext=mp4]/best",  # prefer mp4 container if available
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
    }
    with YoutubeDL(opts) as ydl:
        info = ydl.extract_info(url, download=True)
        # ydl will expand outtmpl, attempt to determine filename
        filename = ydl.prepare_filename(info)
        # if the file does not end with .mp4, rename to .mp4 if the ext is different but mp4 container used
        return filename

async def download_file(url: str, filename: str) -> str:
    """
    Replaces appxdl usage:
    - If the URL looks like a YouTube or other streaming page, uses yt-dlp.
    - Otherwise attempts a direct HTTP(S) stream using aiohttp.
    Returns the path to the downloaded file, or raises Exception on failure.
    """
    url = url.strip()
    url_lower = url.lower()
    # create downloads directory if not exists
    os.makedirs("downloads", exist_ok=True)

    # Decide target extension
    if ".pdf" in url_lower:
        out_name = f"downloads/{filename}.pdf"
    else:
        out_name = f"downloads/{filename}.mp4"

    # If link looks like a direct file link (endswith .mp4 or .pdf or has query with ext), try HTTP download first
    try_http_first = any(url_lower.endswith(ext) for ext in [".mp4", ".pdf"]) or ".pdf?" in url_lower or ".mp4?" in url_lower

    # Heuristic: treat youtube/youtu.be and many known hosts as ytdlp candidates
    ytdlp_hosts = ("youtube.com", "youtu.be", "vimeo.com", "facebook.com", "dailymotion.com", "drive.google.com")
    is_ytdlp = any(h in url_lower for h in ytdlp_hosts) and not url_lower.endswith(".pdf")

    # If URL is obviously a direct link and not a streaming page, do HTTP streaming
    if try_http_first and not is_ytdlp:
        # Attempt HTTP download with retries
        retries = 2
        last_exc = None
        async with aiohttp.ClientSession() as session:
            for attempt in range(retries):
                try:
                    await _download_http_to_file(session, url, out_name)
                    # check file size
                    if os.path.exists(out_name) and os.path.getsize(out_name) > 0:
                        return out_name
                    else:
                        raise Exception("Downloaded file empty")
                except Exception as e:
                    last_exc = e
                    logger.warning(f"HTTP download attempt {attempt+1} failed for {url}: {e}")
                    await asyncio.sleep(2)
            # all http attempts failed; fall through to try yt-dlp if appropriate
            if is_ytdlp:
                logger.info("Falling back to yt-dlp after HTTP failure")
            else:
                raise Exception(f"HTTP download failed for {url}: {last_exc}")

    # If here, try yt-dlp (for streaming pages or as fallback)
    if is_ytdlp or True:
        # Prepare outtmpl - yt-dlp will add proper extension
        # Use a safe temporary template inside downloads dir
        sanitized_template = os.path.join("downloads", filename + ".%(ext)s")
        loop = asyncio.get_event_loop()
        try:
            out_path = await loop.run_in_executor(_thread_pool, _ydl_download_blocking, url, sanitized_template)
            # _ydl_download_blocking returns the actual filename
            if os.path.exists(out_path):
                # If expected extension was .mp4 but ytdlp saved with different ext, and user expects .mp4,
                # optionally rename. We'll keep the original name to avoid corruption.
                return out_path
            else:
                raise Exception("yt-dlp reported file but it does not exist")
        except Exception as e:
            # If both HTTP and yt-dlp fail, raise a clear exception
            raise Exception(f"Download failed for {url}: {e}")

# ... rest of your original code continues unchanged ...

# ─── Upload helper (unchanged from your file) ───────────────────────────────────
async def upload_file_to_channel(
    bot: Client,
    file_path: str,
    caption: str,
    channel_id: int,
    status_msg: Message,
    message_thread_id: Optional[int] = None,
    pyro_target: Optional[int | str] = None,
) -> bool:
    """
    Uploads either .mp4 (with thumbnail) or any other document to the channel.
    Retries up to 3 times on RPCError/FloodWait.
    Ensures that thumbnails are cleaned up after use.
    """
    max_retries = 3

    for attempt in range(max_retries):
        try:
            if file_path.lower().endswith(".mp4"):
                # Extract thumbnail if possible
                thumb = await extract_thumbnail_async(file_path)
                duration = int(await duration_async(file_path))
                try:
                    # If we have a thread id, use Bot API to ensure routing into topic
                    if message_thread_id is not None:
                        try:
                            await bot_api_send_video(channel_id, message_thread_id, file_path, caption, duration=duration, thumb_path=thumb)
                        except Exception as be:
                            if "413" in str(be) or "Request Entity Too Large" in str(be):
                                # Hybrid fallback: upload once to get file_id, then delete and resend by id into topic
                                tmp_target = LOG_CHANNEL_ID or pyro_target or channel_id
                                tmp_msg = await bot.send_video(
                                    chat_id=tmp_target,
                                    video=file_path,
                                    caption=caption,
                                    thumb=thumb,
                                    duration=duration,
                                    supports_streaming=True
                                )
                                file_id = getattr(getattr(tmp_msg, "video", None), "file_id", None)
                                if file_id:
                                    try:
                                        await bot_api_send_video_by_id(channel_id, message_thread_id, file_id, caption, duration=duration)
                                    finally:
                                        try:
                                            await bot.delete_messages(tmp_target, tmp_msg.id)
                                        except Exception:
                                            pass
                                else:
                                    raise
                            else:
                                raise
                    else:
                        await bot.send_video(
                            chat_id=channel_id,
                            video=file_path,
                            caption=caption,
                            thumb=thumb,
                            duration=duration,
                            supports_streaming=True
                        )
                    return True
                finally:
                    if thumb and os.path.exists(thumb):
                        os.remove(thumb)
            else:
                # For non‐video files, send as document
                if message_thread_id is not None:
                    try:
                        await bot_api_send_document(channel_id, message_thread_id, file_path, caption)
                    except Exception as be:
                        if "413" in str(be) or "Request Entity Too Large" in str(be):
                            # Hybrid fallback for documents
                            tmp_target = LOG_CHANNEL_ID or pyro_target or channel_id
                            tmp_msg = await bot.send_document(
                                chat_id=tmp_target,
                                document=file_path,
                                caption=caption
                            )
                            file_id = getattr(getattr(tmp_msg, "document", None), "file_id", None)
                            if file_id:
                                try:
                                    await bot_api_send_document_by_id(channel_id, message_thread_id, file_id, caption)
                                finally:
                                    try:
                                        await bot.delete_messages(tmp_target, tmp_msg.id)
                                    except Exception:
                                        pass
                            else:
                                raise
                        else:
                            raise
                else:
                    await bot.send_document(
                        chat_id=channel_id,
                        document=file_path,
                        caption=caption
                    )
                return True

        except FloodWait as e:
            logger.warning(f"FloodWait during upload: sleeping for {e.value}s")
            await asyncio.sleep(e.value)
            continue

        except RPCError as e:
            logger.error(f"RPCError on upload (attempt {attempt+1}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(2 ** attempt)
            continue

        except Exception as e:
            logger.error(f"Unexpected upload error (attempt {attempt+1}): {e}")
            if attempt == max_retries - 1:
                return False
            await asyncio.sleep(2 ** attempt)

    return False

# ─── Command Handlers ──────────────────────────────────────────────────────────

@app.on_message(filters.command("start") & filters.private)
async def start_handler(client: Client, message: Message):
    user_id = message.from_user.id
    text = (
        "👋 **Welcome to the Subject‐Based Upload Bot!**\n\n"
        "📋 **How to use:**\n"
        "1. Send me a `.txt` file with lines in this format:\n"
        "   `[Subject] Title:URL`\n\n"
        "   - `Subject` (in square brackets) will be used to group uploads.\n"
        "   - `Title` is the human‐readable name (used for filename and caption).\n"
        "   - `URL` is a direct link to the `.mp4` or `.pdf`.\n\n"
        "2. After I process your `.txt`, I'll ask for:\n"
        "   • **Starting line** number\n"
        "   • **Channel ID** (e.g. `-1001234567890`)\n"
        "   • **Batch name** (any text)\n"
        "   • **Downloaded by** (credit text)\n\n"
        "Then I will:\n"
        "  • Read each line from the starting line onward.\n"
        "  • Whenever `[Subject]` changes from the previous one, I'll send a plain message\n"
        "    with that subject and pin it in the channel.\n"
        "  • Upload the corresponding file under that subject with numbered captions.\n"
        "  • Retry failed downloads once before moving to next item.\n\n"
        "🛑 Use `/stop` (in private chat) at any time to halt processing.\n\n"
        f"🆔 Your User ID: `{user_id}`"
    )
    await message.reply_text(text, disable_web_page_preview=True)

@app.on_message(filters.command("stop") & filters.private)
async def stop_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in active_downloads or not active_downloads[user_id]:
        return await message.reply_text("ℹ️ No active process to stop.")
    active_downloads[user_id] = False
    await message.reply_text("⏹️ Processing has been stopped.")

@app.on_message(filters.command("queue") & filters.private)
async def queue_handler(client: Client, message: Message):
    if message.from_user.id not in ALLOWED_USER_IDS:
        return
    jobs = await list_jobs(limit=20)
    if not jobs:
        await message.reply_text("🟢 Queue is empty.")
        return
    lines = []
    for j in jobs:
        jid = str(j.get("_id"))
        status = j.get("status")
        ch = j.get("channel_id")
        total = j.get("total")
        created = datetime.fromtimestamp(j.get("created_at", 0), tz=timezone.utc).strftime('%Y-%m-%d %H:%M UTC')
        lines.append(f"• {jid[:6]}…  [{status}]  ch={ch}  total={total}  at {created}")
    await message.reply_text("Queue:\n" + "\n".join(lines))

@app.on_message(filters.command("cancel") & filters.private)
async def cancel_handler(client: Client, message: Message):
    if message.from_user.id not in ALLOWED_USER_IDS:
        return
    args = message.text.strip().split(maxsplit=1)
    if len(args) < 2:
        await message.reply_text("Usage: /cancel <job_id>")
        return
    job_id = args[1].strip()
    ok = await delete_pending_job(job_id)
    if ok:
        await message.reply_text(f"✅ Canceled job {job_id[:6]}…")
    else:
        await message.reply_text("⚠️ Could not cancel (job not found or already started).")

# ─── Check for incoming .txt files ──────────────────────────────────────────────

def is_txt_document(_, __, message: Message) -> bool:
    doc = message.document
    return bool(doc and doc.file_name and doc.file_name.lower().endswith(".txt"))

@app.on_message(filters.document & filters.create(is_txt_document))
async def txt_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS:
        return

    ack = await message.reply_text("📥 Downloading and reading your .txt file...")
    os.makedirs("downloads", exist_ok=True)
    temp_path = f"downloads/temp_{user_id}.txt"

    try:
        await client.download_media(message, file_name=temp_path)
        with open(temp_path, "r", encoding="utf-8") as f:
            lines = [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"File error: {e}")
        await ack.edit_text("⚠️ Failed to read the file.")
        return
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

    if not lines:
        return await ack.edit_text("⚠️ The file is empty.")

    user_data[user_id] = {
        'lines': lines,
        'total': len(lines),
        'step': 'start_number'
    }
    await ack.edit_text(f"📋 Found {len(lines)} items. Please send the starting line number (1–{len(lines)}).")

# ─── Handle subsequent text inputs (start_number → channel_id → batch_name → downloaded_by) ───────

@app.on_message(filters.text & filters.private)
async def input_handler(client: Client, message: Message):
    user_id = message.from_user.id
    if user_id not in user_data or (ALLOWED_USER_IDS and user_id not in ALLOWED_USER_IDS):
        return

    data = user_data[user_id]
    text = message.text.strip()

    if data['step'] == 'start_number':
        try:
            start = int(text)
            if 1 <= start <= data['total']:
                data['start_number'] = start
                data['step'] = 'channel_id'
                await message.reply_text("📝 Got it. Now send the **channel ID** (e.g. `-1001234567890`).")
            else:
                await message.reply_text(f"❌ Please send a number between 1 and {data['total']}.")
        except ValueError:
            await message.reply_text("❌ That's not a valid integer. Please send the starting line number.")

    elif data['step'] == 'channel_id':
        # Validate channel ID format (starts with -100 for supergroups/channels)
        if not text.startswith("-100"):
            return await message.reply_text("❌ Invalid channel ID format. Make sure it starts with `-100`.")
        data['channel_id'] = int(text)
        data['step'] = 'batch_name'
        await message.reply_text("🏷️ Great! Now send the **batch name** (any text).")

    elif data['step'] == 'batch_name':
        data['batch_name'] = text
        data['step'] = 'downloaded_by'
        await message.reply_text("👤 Perfect! Now send the **Downloaded by** credit text.")

    elif data['step'] == 'downloaded_by':
        data['downloaded_by'] = text
        # Everything is set → enqueue as a job
        try:
            job_id = await enqueue_job(
                user_id=user_id,
                channel_id=int(data['channel_id']),
                lines=data['lines'],
                start_number=int(data['start_number']),
                batch_name=data['batch_name'],
                downloaded_by=data['downloaded_by'],
            )
            if job_id:
                await message.reply_text(
                    f"🗂️ Queued your upload job.\n"
                    f"ID: `{job_id}`\nChannel: `{data['channel_id']}`\nItems: {len(data['lines'])}"
                )
            else:
                await message.reply_text("⚠️ Could not queue job (Mongo unavailable).")
        except Exception as e:
            logger.error(f"Failed to enqueue job: {e}")
            await message.reply_text("❌ Failed to enqueue job.")
            return
        # Kick the worker
        try:
            await _run_job_worker(client)
        except Exception:
            pass
        # Clear interactive state
        user_data.pop(user_id, None)

# ─── Main Processing Loop ────────────────────────────────────────────────────────

async def start_processing(client: Client, message: Message, user_id: int):
    data = user_data[user_id]
    lines = data["lines"]
    start_idx = data["start_number"]
    batch_name = data["batch_name"]
    channel_id = data["channel_id"]
    downloaded_by = data["downloaded_by"]
    total = data["total"]

    # Optional: warm Pyrogram peer resolution using public username (helps hybrid fallback)
    pyro_target = channel_id
    if CHANNEL_USERNAME:
        try:
            await client.get_chat(CHANNEL_USERNAME)
            pyro_target = CHANNEL_USERNAME
            logger.info(f"Warmed Pyrogram peer for {CHANNEL_USERNAME} (will use for hybrid uploads)")
        except Exception as e:
            logger.warning(f"Could not warm Pyrogram peer for {CHANNEL_USERNAME}: {e}")

    # Detect if the target is a forum-enabled supergroup using Bot API
    try:
        chat_info = await bot_api_get_chat(channel_id)
        is_forum = bool(chat_info.get("is_forum", False))
        logger.info(f"Target chat {channel_id}: is_forum={is_forum}")
    except Exception as e:
        is_forum = False
        logger.warning(f"Could not fetch chat info for {channel_id} via Bot API; assuming is_forum=False ({e})")
    subject_threads: Dict[str, int] = {}
    current_thread_id: Optional[int] = None
    # Load persistent cache and prepare per-chat view
    forum_cache = load_forum_cache()
    chat_key = str(channel_id)
    if chat_key not in forum_cache:
        forum_cache[chat_key] = {}
    chat_cache = forum_cache[chat_key]

    # Mark as active
    active_downloads[user_id] = True
    status_msg = await message.reply_text(
        f"🚀 Starting processing:\n"
        f"• Start line: {start_idx}\n"
        f"• Total items: {total}\n"
        f"• Batch name: {batch_name}\n"
        f"• Channel: {channel_id}\n"
        f"• Downloaded by: {downloaded_by}\n\n"
        f"Completed: 0 / {total}"
    )

    processed = 0
    failed = 0
    last_subject = None  # Keep track of the previous subject
    video_count = start_idx - 1  # Continue numbering

    for idx, entry in enumerate(lines[start_idx - 1:], start=start_idx):
        if not active_downloads.get(user_id, True):
            logger.info(f"Process stopped by user {user_id} at line {idx}")
            break

        # Each line is "[Subject] Title:URL"
        if ":" not in entry:
            logger.warning(f"Skipping invalid line {idx}: {entry}")
            failed += 1
            continue

        title_part, url = entry.split(":", 1)
        subjects = extract_subjects(title_part)
        subject = subjects[0]  # We only take the first subject in the list
        subject_norm = _normalize_subject(subject)
        clean_name = clean_title(title_part)

        # If subject changed from last_subject
        if subject != last_subject:
            if is_forum:
                # Create or reuse a forum topic per subject
                try:
                    thread_id = subject_threads.get(subject)
                    if thread_id is None:
                        # Try DB first
                        db_thread = await mongo_get_thread_id(channel_id, subject_norm)
                        if isinstance(db_thread, int):
                            thread_id = db_thread
                            logger.info(f"Reusing DB forum topic '{subject}' with thread_id={thread_id}")
                        # Then local cache
                        if not thread_id:
                            cached = chat_cache.get(subject_norm)
                            if isinstance(cached, int):
                                thread_id = cached
                                logger.info(f"Reusing cached forum topic '{subject}' with thread_id={thread_id}")
                        # If not in cache, create the topic via Bot API (works on Koyeb)
                        if not thread_id:
                            thread_id = await bot_api_create_forum_topic(channel_id, subject)
                        if not thread_id:
                            raise Exception("Could not determine message_thread_id for created topic")
                        subject_threads[subject] = thread_id
                        # Update persistent cache and log
                        chat_cache[subject_norm] = thread_id
                        save_forum_cache(forum_cache)
                        await mongo_set_thread_id(channel_id, subject_norm, thread_id)
                        logger.info(f"Created forum topic '{subject}' with thread_id={thread_id}")
                    else:
                        logger.info(f"Reusing forum topic '{subject}' with thread_id={thread_id}")
                    current_thread_id = thread_id
                    last_subject = subject
                    await asyncio.sleep(1)
                except FloodWait as e:
                    logger.warning(f"FloodWait while creating topic: sleeping for {e.value}s")
                    await asyncio.sleep(e.value)
                except RPCError as e:
                    logger.error(f"Failed to create/use forum topic for '{subject}': {e}")
                    is_forum = False
                    current_thread_id = None
                except Exception as e:
                    logger.error(f"Unexpected error creating forum topic for '{subject}': {e}")
                    is_forum = False
                    current_thread_id = None
            else:
                # Attempt to reuse an existing topic via DB/cache, else create via Bot API
                try:
                    reuse_thread = await mongo_get_thread_id(channel_id, subject_norm)
                    if not reuse_thread:
                        reuse_thread = chat_cache.get(subject_norm)
                    if isinstance(reuse_thread, int):
                        current_thread_id = reuse_thread
                        subject_threads[subject] = reuse_thread
                        last_subject = subject
                        is_forum = True
                        logger.info(f"Reusing cached/DB forum topic '{subject}' with thread_id={reuse_thread} (detection previously false)")
                        await asyncio.sleep(1)
                    else:
                        provisional_thread = await bot_api_create_forum_topic(channel_id, subject)
                        if provisional_thread:
                            logger.info(f"Created forum topic '{subject}' with thread_id={provisional_thread} (detection previously false)")
                            subject_threads[subject] = provisional_thread
                            chat_cache[subject_norm] = provisional_thread
                            save_forum_cache(forum_cache)
                            await mongo_set_thread_id(channel_id, subject_norm, provisional_thread)
                            current_thread_id = provisional_thread
                            last_subject = subject
                            is_forum = True
                            await asyncio.sleep(1)
                        else:
                            raise Exception("No thread id returned")
                except Exception as e:
                    # Non-forum: send a subject header message using Bot API
                    try:
                        await asyncio.sleep(0)  # yield
                        await bot_api_send_message(channel_id, thread_id=0, text=f"📌 {subject}")
                        last_subject = subject
                        current_thread_id = None
                        await asyncio.sleep(1)
                    except Exception as e2:
                        logger.error(f"Failed to send subject header via Bot API: {e2}")

        # Increment video count
        video_count += 1

        # Download the file with retry logic
        item_status = await message.reply_text(f"⬇️ [{idx}/{total}] Downloading: {clean_name}")
        file_path = None
        download_success = False
        
        # Try downloading twice
        for attempt in range(2):
            try:
                url_stripped = url.strip()
                is_pdf = (url_stripped.lower().endswith('.pdf')) or ('.pdf?' in url_stripped.lower())
                base_name = select_pdf_filename(title_part) if is_pdf else clean_name
                file_path = await download_file(url_stripped, base_name)
                download_success = True
                break
            except Exception as e:
                logger.error(f"Download attempt {attempt + 1} failed for line {idx} ({clean_name}): {e}")
                if attempt == 0:  # First attempt failed, try again
                    await item_status.edit_text(f"⚠️ [{idx}/{total}] Download failed, retrying: {clean_name}")
                    await asyncio.sleep(2)  # Wait before retry
                else:  # Second attempt failed
                    await item_status.edit_text(f"❌ [{idx}/{total}] Download failed after retry: {clean_name}")
                    failed += 1

        if not download_success:
            # Fallback: send the styled caption with the original link into the topic/chat
            try:
                fallback_caption = build_caption(
                    subject,
                    idx,
                    title_part,
                    batch_name,
                    downloaded_by,
                    link=url_stripped,
                )
                if is_forum and current_thread_id is not None:
                    await bot_api_send_message(channel_id, thread_id=current_thread_id, text=fallback_caption)
                else:
                    await bot_api_send_message(channel_id, thread_id=0, text=fallback_caption)
            except Exception as e:
                logger.error(f"Failed to send fallback link message for line {idx}: {e}")
            # Move to next item
            continue
            # If it's a YouTube or streaming link, send the URL instead
            if any(x in url.lower() for x in ["youtube.com", "youtu.be", "vimeo.com", "facebook.com", "dailymotion.com"]):
                try:
                    display_title = re.sub(r"\[[^\]]+\]\s*", "", title_part).strip()
                    caption_link = build_caption(subject, video_count, display_title, batch_name, downloaded_by, link=url.strip())
                    await client.send_message(
                        chat_id=channel_id,
                        text=caption_link,
                        message_thread_id=current_thread_id if is_forum else None,
                        disable_web_page_preview=False,
                        reply_to_message_id=None
                    )
                    if is_forum:
                        logger.info(f"Posted fallback link in thread_id={current_thread_id}")
                    logger.info(f"Sent fallback YouTube link for {clean_name}")
                except Exception as e:
                    logger.error(f"Failed to send YouTube fallback link for {clean_name}: {e}")
            else:
                try:
                    await item_status.edit_text(f"❌ [{idx}/{total}] Download failed for {clean_name}")
                except Exception:
                    pass
            failed += 1
            continue

        # Upload under this subject with styled caption
        display_title = re.sub(r"\[[^\]]+\]\s*", "", title_part).strip()
        caption = build_caption(subject, video_count, display_title, batch_name, downloaded_by)
        await item_status.edit_text(f"📤 [{idx}/{total}] Uploading: {clean_name}")
        success = False
        try:
            success = await upload_file_to_channel(
                client,
                file_path,
                caption,
                channel_id,
                item_status,
                message_thread_id=current_thread_id if is_forum else None
            )
            if is_forum:
                logger.info(f"Uploaded to thread_id={current_thread_id} for subject='{subject}'")
        except Exception as e:
            logger.error(f"Unexpected error during upload of '{clean_name}': {e}")
            success = False

        # If upload failed using a cached thread id, attempt to recreate topic once and retry
        if is_forum and not success and subject_threads.get(subject) == chat_cache.get(subject_norm):
            try:
                logger.info(f"Retrying by creating a fresh topic for subject '{subject}' due to failure in cached thread_id")
                new_thread_id = await bot_api_create_forum_topic(channel_id, subject)
                if new_thread_id:
                    subject_threads[subject] = new_thread_id
                    chat_cache[subject_norm] = new_thread_id
                    save_forum_cache(forum_cache)
                    await mongo_set_thread_id(channel_id, subject_norm, new_thread_id)
                    current_thread_id = new_thread_id
                    logger.info(f"Created new topic with thread_id={new_thread_id}; retrying upload")
                    success = await upload_file_to_channel(
                        client,
                        file_path,
                        caption,
                        channel_id,
                        item_status,
                        message_thread_id=new_thread_id,
                        pyro_target=pyro_target
                    )
            except Exception as e:
                logger.error(f"Retry after creating new topic failed: {e}")

        if success:
            logger.info(f"Uploaded '{clean_name}' successfully under '{subject}' as #{video_count}.")
            processed += 1
        else:
            logger.error(f"Upload failed for '{clean_name}' under '{subject}'.")
            failed += 1
            video_count -= 1  # Decrement if upload failed

        # Clean up downloaded file after upload (regardless of success)
        if file_path and os.path.exists(file_path):
            try:
                os.remove(file_path)
            except Exception:
                pass

        # Update status message
        await status_msg.edit_text(
            f"🚀 Processing:\n"
            f"• Current line: {idx}/{total}\n"
            f"• Completed: {processed}\n"
            f"• Failed: {failed}\n"
            f"• Batch: {batch_name}"
        )

        # Rate limiting pause
        if processed % 5 == 0 and processed > 0:
            await asyncio.sleep(10)  # longer sleep every 5 successes
        else:
            await asyncio.sleep(2)   # brief pause between each

        # Delete the item status message
        try:
            await item_status.delete()
        except Exception:
            pass

    # Cleanup
    user_data.pop(user_id, None)
    active_downloads.pop(user_id, None)

    await status_msg.edit_text(
        f"✅ Process completed!\n"
        f"• Successfully uploaded: {processed}\n"
        f"• Failed: {failed}\n"
        f"• Total processed: {processed + failed}"
    )

# ─── Handle potential bad‐time notifications on startup ────────────────────────

def sync_system_time():
    try:
        import subprocess
        subprocess.run(['ntpdate', '-s', 'pool.ntp.org'], timeout=10, capture_output=True)
        logger.info("System time synced")
    except:
        logger.warning("Could not sync system time; proceeding anyway")

if __name__ == "__main__":
    logger.info("Starting bot...")
    try:
        app.run()
    except BadMsgNotification:
        logger.warning("System time mismatch - continuing anyway")
        app.run()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
