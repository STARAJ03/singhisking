import math
import asyncio
import time

# ────────────────────────────────
# 🔧 CONFIGURATION
# ────────────────────────────────
UPDATE_INTERVAL = 3  # seconds between UI updates
BAR_LENGTH = 20      # characters for progress bar


# ────────────────────────────────
# 📊 Utility Functions
# ────────────────────────────────
def human_readable_size(size):
    """Convert bytes into MB/GB format"""
    if size < 1024:
        return f"{size:.2f} B"
    elif size < 1024 ** 2:
        return f"{size / 1024:.2f} KB"
    elif size < 1024 ** 3:
        return f"{size / (1024 ** 2):.2f} MB"
    else:
        return f"{size / (1024 ** 3):.2f} GB"

def progress_bar(percentage):
    """Return a simple progress bar string"""
    filled = int(BAR_LENGTH * percentage / 100)
    empty = BAR_LENGTH - filled
    return f"[{'█' * filled}{'░' * empty}]"

def speed_in_mbps(bytes_done, elapsed_time):
    if elapsed_time <= 0:
        return 0
    return (bytes_done / 1024 / 1024) / elapsed_time


# ────────────────────────────────
# 🚀 Main Progress Handler
# ────────────────────────────────
async def progress_callback(
    current_bytes,
    total_bytes,
    message,
    start_time,
    file_name,
    index,
    total_files,
    next_name=None,
    phase="Downloading"
):
    """Update a single Telegram message with download/upload progress"""
    now = time.time()
    elapsed = now - start_time
    percent = (current_bytes / total_bytes) * 100 if total_bytes else 0
    speed = speed_in_mbps(current_bytes, elapsed)
    done = human_readable_size(current_bytes)
    total = human_readable_size(total_bytes)
    eta = (total_bytes - current_bytes) / (speed * 1024 * 1024) if speed > 0 else 0

    # Build progress text
    progress_text = (
        f"📦 **{phase} File:** {file_name}\n"
        f"📊 **Progress:** {progress_bar(percent)} `{percent:.1f}%`\n"
        f"📁 **Size:** {done} / {total}\n"
        f"⚡ **Speed:** {speed:.2f} MB/s\n"
        f"⏱️ **ETA:** {int(eta)}s\n"
        f"📂 **Queue:** {index}/{total_files}\n"
    )

    if next_name:
        progress_text += f"➡️ **Next:** {next_name}\n"

    try:
        await message.edit_text(progress_text)
    except Exception:
        pass


# ────────────────────────────────
# 🧵 Stream Tracker for aiohttp or uploads
# ────────────────────────────────
async def track_progress(
    message,
    file_name,
    total_bytes,
    reader,
    writer,
    index,
    total_files,
    next_name=None,
    phase="Downloading"
):
    """Reads chunks and periodically updates Telegram progress"""
    start_time = time.time()
    last_update = time.time()
    bytes_done = 0
    chunk_size = 1024 * 64  # 64KB

    while True:
        chunk = await reader.read(chunk_size)
        if not chunk:
            break
        writer.write(chunk)
        bytes_done += len(chunk)
        if time.time() - last_update > UPDATE_INTERVAL:
            await progress_callback(
                bytes_done,
                total_bytes,
                message,
                start_time,
                file_name,
                index,
                total_files,
                next_name,
                phase
            )
            last_update = time.time()

    await progress_callback(
        bytes_done,
        total_bytes,
        message,
        start_time,
        file_name,
        index,
        total_files,
        next_name,
        phase
    )
