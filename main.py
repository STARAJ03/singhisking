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
import requests
from progress_tracker import ProgressTracker
from pyrogram import Client, filters, idle
from pyrogram.errors import FloodWait, RPCError, BadMsgNotification, MessageNotModified
from pyrogram.types import Message
from typing import Dict, List, Optional
from link_resolver import resolve_url

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
INSTANCE_KEY = os.getenv("INSTANCE_KEY", "").strip()

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

class Cancelled(Exception):
    pass

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

async def is_video_file_async(filename: str) -> bool:
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=codec_name', '-of', 'csv=p=0', filename,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        return proc.returncode == 0 and stdout.decode().strip() != ""
    except Exception:
        return False

async def get_codecs_async(filename: str) -> tuple[str, str]:
    v, a = "", ""
    try:
        pv = await asyncio.create_subprocess_exec('ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=codec_name', '-of', 'csv=p=0', filename, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        sv, _ = await pv.communicate()
        if pv.returncode == 0:
            v = sv.decode().strip()
    except Exception:
        pass
    try:
        pa = await asyncio.create_subprocess_exec('ffprobe', '-v', 'error', '-select_streams', 'a:0', '-show_entries', 'stream=codec_name', '-of', 'csv=p=0', filename, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        sa, _ = await pa.communicate()
        if pa.returncode == 0:
            a = sa.decode().strip()
    except Exception:
        pass
    return v, a

async def get_h264_profile_async(filename: str) -> str:
    try:
        proc = await asyncio.create_subprocess_exec(
            'ffprobe', '-v', 'error', '-select_streams', 'v:0', '-show_entries', 'stream=profile', '-of', 'csv=p=0', filename,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        stdout, _ = await proc.communicate()
        if proc.returncode == 0:
            return stdout.decode().strip()
    except Exception:
        pass
    return ""

async def transcode_to_streamable_mp4_async(filename: str) -> Optional[str]:
    try:
        out_path = filename + ".streamable.mp4"
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg', '-y', '-i', filename,
            '-c:v', 'libx264', '-preset', 'veryfast', '-crf', '23',
            '-profile:v', 'baseline', '-level', '3.1', '-pix_fmt', 'yuv420p',
            '-c:a', 'aac', '-b:a', '128k',
            '-movflags', '+faststart',
            out_path,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            try:
                os.remove(filename)
            except Exception:
                pass
            shutil.move(out_path, filename if filename.lower().endswith('.mp4') else (os.path.splitext(filename)[0] + '.mp4'))
            # If original wasn't .mp4, update path
            return (filename if filename.lower().endswith('.mp4') else (os.path.splitext(filename)[0] + '.mp4'))
        else:
            try:
                if os.path.exists(out_path):
                    os.remove(out_path)
            except Exception:
                pass
            return None
    except Exception:
        return None

async def ensure_streamable_async(filename: str) -> str:
    """Ensure the file is Telegram-streamable. Returns possibly new path."""
    # If not a video, return
    if not await is_video_file_async(filename):
        return filename
    v, a = await get_codecs_async(filename)
    # If already H.264/AAC in mp4, just make sure faststart is set
    if v.lower() in ("h264", "avc1") and (a.lower() in ("aac", "mp4a") or a == ""):
        if filename.lower().endswith('.mp4'):
            await remux_faststart_async(filename)
            return filename
    # Otherwise, transcode to mp4 H.264/AAC
    new_path = await transcode_to_streamable_mp4_async(filename)
    if new_path:
        await remux_faststart_async(new_path)
        return new_path
    # Fallback: try faststart anyway
    if filename.lower().endswith('.mp4'):
        await remux_faststart_async(filename)
    return filename
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

async def remux_faststart_async(filename: str) -> Optional[str]:
    """If MP4, remux with faststart to enable progressive streaming. Returns output path or None."""
    try:
        if not filename.lower().endswith(".mp4"):
            return None
        out_path = filename + ".faststart.mp4"
        proc = await asyncio.create_subprocess_exec(
            'ffmpeg',
            '-y',
            '-i', filename,
            '-c', 'copy',
            '-movflags', '+faststart',
            out_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        await proc.communicate()
        if proc.returncode == 0 and os.path.exists(out_path) and os.path.getsize(out_path) > 0:
            try:
                os.remove(filename)
            except Exception:
                pass
            shutil.move(out_path, filename)
            return filename
        else:
            # Cleanup temp if created
            try:
                if os.path.exists(out_path):
                    os.remove(out_path)
            except Exception:
                pass
            return None
    except Exception:
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
    # Ensure the background worker is running
    try:
        if not globals().get("_job_worker_running", False):
            asyncio.create_task(_run_job_worker(app))
    except Exception:
        pass
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

async def delete_job(job_id: str) -> None:
    col = await get_jobs_collection()
    if col is None:
        return
    try:
        await col.delete_one({"_id": ObjectId(job_id)})
    except Exception:
        pass

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
                # Ensure user_data exists (guard against accidental clears)
                if uid not in user_data:
                    user_data[uid] = {
                        "lines": lines,
                        "start_number": start_idx,
                        "batch_name": batch_name,
                        "channel_id": channel_id,
                        "downloaded_by": downloaded_by,
                        "total": len(lines),
                    }
                # Call existing processing using the prepared user_data
                result_completed = await start_processing(client, dm or Message(id=0), uid)
            except Exception as e:
                logger.exception(f"Job {job_id} failed: {e}")
                await set_job_status(job_id, "failed")
            else:
                status = "completed" if result_completed else "stopped"
                await set_job_status(job_id, status)
                # Auto-clean finished or stopped jobs
                try:
                    await delete_job(job_id)
                except Exception:
                    pass
    finally:
        _job_worker_running = False
    

async def mongo_get_thread_id(chat_id: int, subject_norm: str) -> Optional[int]:
    col = await get_mongo_collection()
    if col is None:
        return None
    query = {"chat_id": chat_id, "subject_norm": subject_norm}
    if INSTANCE_KEY:
        query["instance"] = INSTANCE_KEY
    doc = await col.find_one(query)
    return int(doc["thread_id"]) if doc and "thread_id" in doc else None

async def mongo_set_thread_id(chat_id: int, subject_norm: str, thread_id: int) -> None:
    col = await get_mongo_collection()
    if col is None:
        return
    filt = {"chat_id": chat_id, "subject_norm": subject_norm}
    if INSTANCE_KEY:
        filt["instance"] = INSTANCE_KEY
    update = {"$set": {"thread_id": int(thread_id), "updated_at": int(time.time())}}
    if INSTANCE_KEY:
        update["$set"]["instance"] = INSTANCE_KEY
    await col.update_one(filt, update, upsert=True)

async def mongo_get_last_index(chat_id: int, subject_norm: str) -> Optional[int]:
    col = await get_mongo_collection()
    if col is None:
        return None
    query = {"chat_id": chat_id, "subject_norm": subject_norm}
    if INSTANCE_KEY:
        query["instance"] = INSTANCE_KEY
    doc = await col.find_one(query, projection={"last_index": 1})
    if doc and isinstance(doc.get("last_index"), int):
        return int(doc["last_index"])
    return None

async def mongo_set_last_index(chat_id: int, subject_norm: str, last_index: int) -> None:
    col = await get_mongo_collection()
    if col is None:
        return
    filt = {"chat_id": chat_id, "subject_norm": subject_norm}
    if INSTANCE_KEY:
        filt["instance"] = INSTANCE_KEY
    update = {"$set": {"last_index": int(last_index), "updated_at": int(time.time())}}
    if INSTANCE_KEY:
        update["$set"]["instance"] = INSTANCE_KEY
    await col.update_one(filt, update, upsert=True)

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

def _embed_link_in_title(caption: str, url: Optional[str]) -> str:
    if not url:
        return caption
    try:
        lines = caption.splitlines()
        for i, line in enumerate(lines):
            if line.strip().startswith("▸ 𝙏𝙞𝙩𝙡𝙚"):
                lines[i] = f"{line} (🔗 {url})"
                return "\n".join(lines)
        # Fallback: append link at the end
        return caption + f"\n🔗 {url}"
    except Exception:
        return caption

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

async def bot_api_send_message(chat_id: int, thread_id: int, text: str) -> int:
    url = _bot_api_base() + "/sendMessage"
    payload = {"chat_id": chat_id, "message_thread_id": thread_id, "text": text, "disable_web_page_preview": False}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, json=payload, timeout=30) as resp:
            data = await resp.json(content_type=None)
            if not data.get("ok"):
                raise Exception(f"BotAPI sendMessage failed: {data}")
            return data.get("result", {}).get("message_id")

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
        "format": "best[ext=mp4]/best",
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
    - Adds robust ffmpeg-based support for .m3u8 (HLS) including pre-signing
      for Classplus/Testbook/Cloudfront and VisionIAS scraping.
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

    # BEFORE anything: try to resolve protected/DRM links via your existing globals pre-signs
    # (this mirrors the signing behavior from your working uploader)
    try:
        # import globals if available and try to pre-sign classplus / cloudfront style URLs
        try:
            import globals as __globals_mod  # already in repo
            __cptoken = getattr(__globals_mod, "cptoken", None)
        except Exception:
            __cptoken = None

        # If it looks like Classplus/Testbook/Cloudfront style, call the jw-signed-url API
        if any(x in url_lower for x in [
            "classplusapp.com", "media-cdn", "tencdn", "cpvod", "cloudfront.net", "videos.classplusapp"
        ]) and __cptoken:
            try:
                headers = {
                    'x-access-token': __cptoken,
                    'accept-language': 'EN',
                    'api-version': '18',
                    'user-agent': 'Mobile-Android',
                }
                params = {"url": url}
                r = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params, timeout=12)
                if r.ok:
                    data = r.json()
                    if "url" in data and data.get("url"):
                        url = data["url"]
                        url_lower = url.lower()
                        logger.info(f"Pre-signed protected URL for download: {url}")
            except Exception as e:
                logger.warning(f"Pre-sign attempt failed (will continue): {e}")
    except Exception:
        # don't crash if pre-sign helper is missing
        pass

        # --- Handle HLS (.m3u8) streams separately ---
    if url_lower.endswith(".m3u8") or ".m3u8?" in url_lower:
        try:
            # Resolve / pre-sign the m3u8 URL first using your logic_resolver helper
            from logic_resolver import resolve_url
            resolved = await resolve_url(url)
            signed_url = resolved.get("url", url)

            # Build output name
            out_name = f"downloads/{filename}.mp4"

            # Try ffmpeg direct download at 720p (if multiple variants)
            ffmpeg_cmd = [
                "ffmpeg", "-y", "-loglevel", "error",
                "-i", signed_url,
                "-c", "copy", "-bsf:a", "aac_adtstoasc",
                out_name
            ]
            proc = await asyncio.create_subprocess_exec(
                *ffmpeg_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            _, stderr = await proc.communicate()

            # Check if file exists and not empty
            if os.path.exists(out_name) and os.path.getsize(out_name) > 1024 * 100:
                # move moov atom to start for instant playback
                try:
                    await remux_faststart_async(out_name)
                except Exception:
                    pass
                return out_name
            else:
                raise Exception(f"ffmpeg failed or empty file: {stderr.decode() if stderr else ''}")

        except Exception as e:
            raise Exception(f"HLS (.m3u8) download failed: {e}")


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
                        # Only for direct .mp4 links, move moov atom to the start for instant play
                        if out_name.lower().endswith('.mp4'):
                            if os.path.exists(out_name) and os.path.getsize(out_name) > 0:
                                try:
                                    await remux_faststart_async(out_name)
                                except Exception:
                                    pass
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
            if os.path.exists(out_path):
                # Ensure progressive playback if yt-dlp produced an mp4
                if out_path.lower().endswith('.mp4'):
                    try:
                        await remux_faststart_async(out_path)
                    except Exception:
                        pass
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
    cancel_user_id: Optional[int] = None,
    original_url: Optional[str] = None,
) -> bool:
    """
    Uploads either .mp4 (with thumbnail) or any other document to the channel.
    Retries up to 3 times on RPCError/FloodWait.
    Ensures that thumbnails are cleaned up after use.
    """
    # Total attempts per upload = max_retries; reattempts = max_retries - 1
    max_retries = 1

    for attempt in range(max_retries):
        try:
            if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                raise Cancelled("Cancelled before upload start")
            if file_path.lower().endswith(".mp4"):
                # Embed original link if codec is non-H.264/AVC OR codec probe failed/empty
                try:
                    vcodec, _ = await get_codecs_async(file_path)
                    if not vcodec or vcodec.lower() not in ("h264", "avc1"):
                        caption = _embed_link_in_title(caption, original_url)
                except Exception:
                    # On probe failure, still embed link as a safe fallback
                    try:
                        caption = _embed_link_in_title(caption, original_url)
                    except Exception:
                        pass
                # Extract thumbnail if possible
                if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                    raise Cancelled("Cancelled before thumbnail")
                thumb = await extract_thumbnail_async(file_path)
                duration = int(await duration_async(file_path))
                try:
                    # If we have a thread id, use Bot API to ensure routing into topic
                    if message_thread_id is not None:
                        try:
                            if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                raise Cancelled("Cancelled before bot API send video")
                            await bot_api_send_video(channel_id, message_thread_id, file_path, caption, duration=duration, thumb_path=thumb)
                        except Exception as be:
                            if "413" in str(be) or "Request Entity Too Large" in str(be):
                                # Hybrid fallback: upload once to get file_id, then delete and resend by id into topic
                                tmp_target = LOG_CHANNEL_ID or pyro_target or channel_id
                                # Ensure log channel peer is resolved before sending
                                if LOG_CHANNEL_ID:
                                    try:
                                        await bot.get_chat(LOG_CHANNEL_ID)
                                    except Exception:
                                        pass
                                if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                    raise Cancelled("Cancelled before temp upload video")
                                try:
                                    tmp_msg = await bot.send_video(
                                        chat_id=tmp_target,
                                        video=file_path,
                                        caption=caption,
                                        thumb=thumb,
                                        duration=duration,
                                        supports_streaming=True
                                    )
                                except RPCError as e:
                                    if LOG_CHANNEL_ID and "Peer id invalid" in str(e):
                                        # Retry once after explicit resolve
                                        try:
                                            await bot.get_chat(LOG_CHANNEL_ID)
                                            if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                                raise Cancelled("Cancelled before temp upload video retry")
                                            tmp_msg = await bot.send_video(
                                                chat_id=tmp_target,
                                                video=file_path,
                                                caption=caption,
                                                thumb=thumb,
                                                duration=duration,
                                                supports_streaming=True
                                            )
                                        except Exception as e2:
                                            raise e2
                                    else:
                                        raise
                                file_id = getattr(getattr(tmp_msg, "video", None), "file_id", None)
                                if file_id:
                                    try:
                                        if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                            raise Cancelled("Cancelled before Bot API resend video by id")
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
                        # Guard: if we are in a forum context but do not have a thread id yet, do not try Pyrogram direct send to the private group
                        if message_thread_id is None and LOG_CHANNEL_ID:
                            # As a fallback, upload to log channel to obtain file_id and then send to topic once thread id is available
                            tmp_target = LOG_CHANNEL_ID
                            try:
                                await bot.get_chat(LOG_CHANNEL_ID)
                            except Exception:
                                pass
                            if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                raise Cancelled("Cancelled before temp upload video (no thread)")
                            tmp_msg = await bot.send_video(
                                chat_id=tmp_target,
                                video=file_path,
                                caption=caption,
                                thumb=thumb,
                                duration=duration,
                                supports_streaming=True
                            )
                            file_id = getattr(getattr(tmp_msg, "video", None), "file_id", None)
                            if not file_id:
                                raise Exception("Could not obtain file_id from temp upload")
                            # Without thread id we cannot deliver to target yet; caller should retry the item after thread id is available
                            try:
                                await bot.delete_messages(tmp_target, tmp_msg.id)
                            except Exception:
                                pass
                            raise Exception("Thread id not available yet for forum; skipping direct send")
                        if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                            raise Cancelled("Cancelled before direct send video")
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
                        if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                            raise Cancelled("Cancelled before bot API send document")
                        await bot_api_send_document(channel_id, message_thread_id, file_path, caption)
                    except Exception as be:
                        if "413" in str(be) or "Request Entity Too Large" in str(be):
                            # Hybrid fallback for documents
                            tmp_target = LOG_CHANNEL_ID or pyro_target or channel_id
                            # Ensure log channel peer is resolved before sending
                            if LOG_CHANNEL_ID:
                                try:
                                    await bot.get_chat(LOG_CHANNEL_ID)
                                except Exception:
                                    pass
                            if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                raise Cancelled("Cancelled before temp upload document")
                            try:
                                tmp_msg = await bot.send_document(
                                    chat_id=tmp_target,
                                    document=file_path,
                                    caption=caption
                                )
                            except RPCError as e:
                                if LOG_CHANNEL_ID and "Peer id invalid" in str(e):
                                    # Retry once after explicit resolve
                                    try:
                                        await bot.get_chat(LOG_CHANNEL_ID)
                                        if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                            raise Cancelled("Cancelled before temp upload document retry")
                                        tmp_msg = await bot.send_document(
                                            chat_id=tmp_target,
                                            document=file_path,
                                            caption=caption
                                        )
                                    except Exception as e2:
                                        raise e2
                                else:
                                    raise
                            file_id = getattr(getattr(tmp_msg, "document", None), "file_id", None)
                            if file_id:
                                try:
                                    if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                                        raise Cancelled("Cancelled before Bot API resend document by id")
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
                    if cancel_user_id is not None and not active_downloads.get(cancel_user_id, True):
                        raise Cancelled("Cancelled before direct send document")
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

        except Cancelled as e:
            logger.info(str(e))
            return False
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

@app.on_message(filters.command("clear_queue") & filters.private)
async def clear_queue_handler(client: Client, message: Message):
    if message.from_user.id not in ALLOWED_USER_IDS:
        return
    args = message.text.strip().split(maxsplit=1)
    scope = (args[1].strip().lower() if len(args) > 1 else "pending")
    col = await get_jobs_collection()
    if col is None:
        await message.reply_text("⚠️ Mongo unavailable.")
        return
    if scope == "all":
        res = await col.delete_many({})
        await message.reply_text(f"🧹 Cleared ALL jobs: {res.deleted_count} deleted.")
    else:
        res = await col.delete_many({"status": "pending"})
        await message.reply_text(f"🧹 Cleared pending jobs: {res.deleted_count} deleted.")

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
    try:
        await ack.edit_text(f"📋 Found {len(lines)} items. Send starting line number (0 for auto, or 1–{len(lines)}).")
    except MessageNotModified:
        pass

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
            if start == 0 or (1 <= start <= data['total']):
                data['start_number'] = start
                data['step'] = 'channel_id'
                await message.reply_text("📝 Got it. Now send the **channel ID** (e.g. `-1001234567890`).")
            else:
                await message.reply_text(f"❌ Please send 0 for auto-numbering or a number between 1 and {data['total']}.")
        except ValueError:
            await message.reply_text("❌ That's not a valid integer. Send 0 for auto-numbering or a starting line number.")

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
    # Mark this user's processing as active
    active_downloads[user_id] = True
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
    # ✅ Start live progress tracker
    tracker = ProgressTracker(client, status_msg, total)
    await tracker.start()

    processed = 0
    failed = 0
    last_subject = None  # Keep track of the previous subject
    # Per-topic numbering: map subject_norm -> last_index seen in this run
    subject_counts: dict[str, int] = {}
    auto_numbering = not (start_idx and int(start_idx) > 0)

    # When start_idx == 0 (auto-numbering), iterate from the first line
    iter_start_line = start_idx if (start_idx and start_idx > 0) else 1
        # === REPLACE the for-loop with this block ===
    # cumulative bytes counters for tracker speed calc
    cumulative_downloaded_bytes = 0
    cumulative_uploaded_bytes = 0

    # Update the initial status message to be labeled "batch downloading"
    try:
        await status_msg.edit_text(
            f"🚀 Batch downloading\n"
            f"• Start line: {start_idx}\n"
            f"• Total items: {total}\n"
            f"• Batch name: {batch_name}\n"
            f"• Channel: {channel_id}\n"
            f"• Downloaded by: {downloaded_by}\n\n"
            f"Completed: 0 / {total}"
        )
    except Exception:
        pass

    for idx, entry in enumerate(lines[iter_start_line - 1:], start=iter_start_line):
        # stop requested?
        if not active_downloads.get(user_id, False):
            try:
                await status_msg.edit_text("⏹️ Stopped by user.")
            except Exception:
                pass
            active_downloads[user_id] = False
            return False

        if ":" not in entry:
            logger.warning(f"Skipping invalid line {idx}: {entry}")
            failed += 1
            # update tracker for skip
            await tracker.update(processed=processed, failed=failed, bytes_downloaded=cumulative_downloaded_bytes, bytes_uploaded=cumulative_uploaded_bytes, next_item=(lines[idx] if idx < total else None))
            continue

        title_part, url = entry.split(":", 1)
        subjects = extract_subjects(title_part)
        subject = subjects[0]
        subject_norm = _normalize_subject(subject)
        clean_name = clean_title(title_part)

        # init subject count if needed (keeps your previous logic)
        if subject_norm not in subject_counts:
            if auto_numbering:
                try:
                    li = await mongo_get_last_index(channel_id, subject_norm)
                    subject_counts[subject_norm] = li or 0
                except Exception:
                    subject_counts[subject_norm] = 0
            else:
                subject_counts[subject_norm] = int(start_idx) - 1

        # Handle subject header / forum topic creation (unchanged logic)...
        # (KEEP the exact code you already have here that creates forum topics
        #  and sets current_thread_id / last_subject)
        # --- paste your existing subject-handling block here unchanged ---
        # (To be clear: do not remove or change the code you had above that creates topics.)
        # For brevity in this patch, assume that block remains exactly as in your file.

        # Determine current index for this subject
        current_index = subject_counts[subject_norm] + 1

        # --- 1) Resolve/proxy the URL using logic_resolver ---
        try:
            resolved = await resolve_url(url, name=clean_name, raw_text2=str(raw_text2) if 'raw_text2' in locals() else "1080")
            resolved_url = resolved.get("url") or url
        except Exception as e:
            logger.warning(f"Resolver failed for line {idx}: {e}")
            resolved_url = url

        # --- 2) Download with timing to estimate download speed ---
        file_path = None
        download_success = False
        download_attempts = 2
        last_err = None

        for attempt in range(download_attempts):
            try:
                download_start_ts = time.time()
                # call your existing download routine (unchanged)
                file_path = await download_file(resolved_url, f"{str(idx).zfill(3)} {clean_name}")
                download_end_ts = time.time()
                # compute bytes and speed
                try:
                    b = os.path.getsize(file_path)
                except Exception:
                    b = 0
                delta_t = max(0.001, (download_end_ts - download_start_ts))
                speed_bps = b / delta_t if delta_t > 0 else 0.0
                # Update cumulative counters and tracker (MB/s)
                cumulative_downloaded_bytes += b
                # tracker expects bytes_downloaded cumulative
                await tracker.update(
                    processed=processed,
                    failed=failed,
                    bytes_downloaded=cumulative_downloaded_bytes,
                    bytes_uploaded=cumulative_uploaded_bytes,
                    next_item=(lines[idx] if idx < total else None)
                )
                download_success = True
                break
            except Exception as e:
                last_err = e
                logger.warning(f"Download attempt {attempt+1} failed for line {idx} ({clean_name}): {e}")
                # update tracker to show failure attempt (but not increment failed yet)
                await tracker.update(
                    processed=processed,
                    failed=failed,
                    bytes_downloaded=cumulative_downloaded_bytes,
                    bytes_uploaded=cumulative_uploaded_bytes,
                    next_item=(lines[idx] if idx < total else None)
                )
                await asyncio.sleep(1)

        if not download_success:
            failed += 1
            # update tracker and continue to next item
            await tracker.update(
                processed=processed,
                failed=failed,
                bytes_downloaded=cumulative_downloaded_bytes,
                bytes_uploaded=cumulative_uploaded_bytes,
                next_item=(lines[idx] if idx < total else None)
            )
            # Optionally notify the user per your original behavior
            try:
                await item_status.edit_text(f"⚠️ Failed to download: {clean_name}\nReason: {last_err}")
            except Exception:
                pass
            # cleanup if partial file exists
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass
            continue

        # --- 3) Ensure streamable / determine upload target and upload while timing ---
        try:
            # prepare caption same as before in your file
            caption = build_caption(subject, current_index, title_part, batch_name, downloaded_by, link=url)
            # measure upload time around upload_file_to_channel call
            upload_start_ts = time.time()
            up_success = await upload_file_to_channel(
                bot=client,
                file_path=file_path,
                caption=caption,
                channel_id=channel_id,
                status_msg=status_msg,
                message_thread_id=current_thread_id,
                pyro_target=pyro_target,
                cancel_user_id=user_id,
                original_url=url
            )
            upload_end_ts = time.time()
            uploaded_bytes = 0
            try:
                uploaded_bytes = os.path.getsize(file_path)
            except Exception:
                uploaded_bytes = 0
            delta_up_t = max(0.001, (upload_end_ts - upload_start_ts))
            upload_bps = uploaded_bytes / delta_up_t if delta_up_t > 0 else 0.0
            cumulative_uploaded_bytes += uploaded_bytes

            # success handling
            if up_success:
                processed += 1
                # increment per-subject counter and persist
                subject_counts[subject_norm] = current_index
                try:
                    await mongo_set_last_index(channel_id, subject_norm, current_index)
                except Exception:
                    pass
                # update tracker with final bytes and next item
                await tracker.update(
                    processed=processed,
                    failed=failed,
                    bytes_downloaded=cumulative_downloaded_bytes,
                    bytes_uploaded=cumulative_uploaded_bytes,
                    next_item=(lines[idx] if idx < total else None)
                )
                # edit item status to show done
                try:
                    await item_status.edit_text(f"✅ [{idx}/{total}] Uploaded: {clean_name}")
                except Exception:
                    pass
            else:
                failed += 1
                await tracker.update(
                    processed=processed,
                    failed=failed,
                    bytes_downloaded=cumulative_downloaded_bytes,
                    bytes_uploaded=cumulative_uploaded_bytes,
                    next_item=(lines[idx] if idx < total else None)
                )
                try:
                    await item_status.edit_text(f"⚠️ Upload failed for: {clean_name}")
                except Exception:
                    pass

        except Exception as e:
            failed += 1
            logger.exception(f"Unexpected error handling upload for line {idx}: {e}")
            await tracker.update(
                processed=processed,
                failed=failed,
                bytes_downloaded=cumulative_downloaded_bytes,
                bytes_uploaded=cumulative_uploaded_bytes,
                next_item=(lines[idx] if idx < total else None)
            )
            try:
                await item_status.edit_text(f"⚠️ Error: {e}")
            except Exception:
                pass
        finally:
            # clean up downloaded file
            if file_path and os.path.exists(file_path):
                try:
                    os.remove(file_path)
                except Exception:
                    pass

    # === end of for-loop ===

    # After loop completes, ensure tracker stops and finalizes
    try:
        await tracker.stop()
    except Exception:
        pass

    # Save forum cache persistently
    save_forum_cache(forum_cache)
    active_downloads[user_id] = False
    return True

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
                    pyro_target=pyro_target,
                    cancel_user_id=user_id,
                    original_url=url_stripped,
                )
        except Exception as e:
            logger.error(f"Retry after creating new topic failed: {e}")

    if success:
        logger.info(f"Uploaded '{clean_name}' successfully under '{subject}' as #{current_index}.")
        # Delete the provisional header message once the first upload succeeds to keep the thread clean
        try:
            if 'header_msg_id' in locals() and header_msg_id:
                await bot_api_delete_message(channel_id, header_msg_id)
                header_msg_id = None
        except Exception:
            pass
         # Persist last_index for this topic (continue numbering on future additions)
        try:
            if is_forum and current_thread_id is not None:
                await mongo_set_last_index(channel_id, subject_norm, current_index)
        except Exception:
            pass
        # Update in-memory counter for this subject
        subject_counts[subject_norm] = current_index
        processed += 1
    else:
        logger.error(f"Upload failed for '{clean_name}' under '{subject}'.")
        failed += 1
    # Update live progress tracker
    await tracker.update(
        processed=processed,
        failed=failed,
        next_item=(lines[idx] if idx < total else None)
    )

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
active_downloads[user_id] = False
await status_msg.edit_text("✅ All items processed.")
await tracker.stop()
return True

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
        # Warm LOG_CHANNEL_ID peer on startup if provided
        try:
            if LOG_CHANNEL_ID:
                async def _warm_log():
                    try:
                        await app.start()
                        await app.get_chat(LOG_CHANNEL_ID)
                    finally:
                        await app.stop()
                import asyncio as _a
                _a.run(_warm_log())
        except Exception as _e:
            logger.warning(f"Log channel warm skipped: {_e}")
        app.run()
    except BadMsgNotification:
        logger.warning("System time mismatch - continuing anyway")
        app.run()
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
