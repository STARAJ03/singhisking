import math
import asyncio
import time

# ────────────────────────────────
# 🔧 CONFIGURATION
# ────────────────────────────────
UPDATE_INTERVAL = 5   # seconds between UI updates
BAR_LENGTH = 20       # characters for progress bar
MIN_PERCENT_STEP = 3  # update only if +3% progress since last edit


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
# 🚀 Main Progress Handler (optimized)
# ────────────────────────────────
_last_percent_cache = {}  # prevent over-editing per message


async def progress_callback(
    current, total, message, start_time, file_name,
    phase="Uploading", *args, **kwargs
):
    """Update a single Telegram message with download/upload progress"""

    try:
        start_time = float(start_time)
    except Exception:
        start_time = time.time()

    current_bytes = current
    total_bytes = total
    now = time.time()
    elapsed = now - start_time

    percent = (current_bytes / total_bytes) * 100 if total_bytes else 0
    speed = speed_in_mbps(current_bytes, elapsed)
    done = human_readable_size(current_bytes)
    total_hr = human_readable_size(total_bytes)
    eta = (total_bytes - current_bytes) / (speed * 1024 * 1024) if speed > 0 else 0

    # 🧠 Skip update if less than 3% change since last message
    last_percent = _last_percent_cache.get(message.chat.id, 0)
    if percent - last_percent < MIN_PERCENT_STEP and percent < 100:
        return
    _last_percent_cache[message.chat.id] = percent

    # Build progress text (no queue info)
    progress_text = (
        f"📦 **{phase} File:** {file_name}\n"
        f"📊 **Progress:** {progress_bar(percent)} `{percent:.1f}%`\n"
        f"📁 **Size:** {done} / {total_hr}\n"
        f"⚡ **Speed:** {speed:.2f} MB/s\n"
        f"⏱️ **ETA:** {int(eta)}s\n"
    )

    try:
        await message.edit_text(progress_text)
    except Exception:
        pass


# ────────────────────────────────
# 🧵 Stream Tracker for aiohttp or manual file writes
# ────────────────────────────────
async def track_progress(
    message,
    file_name,
    total_bytes,
    reader,
    writer,
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

        # Update progress only every few seconds (or large jumps)
        if time.time() - last_update > UPDATE_INTERVAL:
            await progress_callback(
                bytes_done,
                total_bytes,
                message,
                start_time,
                file_name,
                phase
            )
            last_update = time.time()

    # Final 100% update
    await progress_callback(
        bytes_done,
        total_bytes,
        message,
        start_time,
        file_name,
        phase
    )
