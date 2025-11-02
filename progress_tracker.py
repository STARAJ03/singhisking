# progress_tracker.py
import asyncio
import time
import math
from typing import Optional
from pyrogram.types import Message


class ProgressTracker:
    """
    Tracks and updates Telegram progress messages for ongoing batch operations.
    Shows:
      • Progress bar
      • Processed / Total
      • Remaining
      • Download/Upload speed
      • Failed count
      • Next item in queue
    """

    def __init__(self, client, message: Message, total: int):
        self.client = client
        self.message = message
        self.total = total
        self.processed = 0
        self.failed = 0
        self.last_bytes_downloaded = 0
        self.last_bytes_uploaded = 0
        self.last_time = time.time()
        self.download_speed = 0.0
        self.upload_speed = 0.0
        self.next_item: Optional[str] = None
        self._stop_flag = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()

    async def start(self):
        """Starts background updater task."""
        if self._task is None:
            self._stop_flag = False
            self._task = asyncio.create_task(self._run())

    async def stop(self):
        """Stops background updater."""
        self._stop_flag = True
        if self._task:
            await asyncio.sleep(0.1)
            self._task.cancel()
            self._task = None
        try:
            await self._render(final=True)
        except Exception:
            pass

    async def update(
        self,
        processed: Optional[int] = None,
        failed: Optional[int] = None,
        bytes_downloaded: Optional[int] = None,
        bytes_uploaded: Optional[int] = None,
        next_item: Optional[str] = None,
    ):
        """Update internal counters (called by main loop)."""
        async with self._lock:
            now = time.time()
            dt = now - self.last_time if now > self.last_time else 1.0
            if bytes_downloaded is not None:
                self.download_speed = (bytes_downloaded - self.last_bytes_downloaded) / dt
                self.last_bytes_downloaded = bytes_downloaded
            if bytes_uploaded is not None:
                self.upload_speed = (bytes_uploaded - self.last_bytes_uploaded) / dt
                self.last_bytes_uploaded = bytes_uploaded
            if processed is not None:
                self.processed = processed
            if failed is not None:
                self.failed = failed
            if next_item is not None:
                self.next_item = next_item
            self.last_time = now

    async def _run(self):
        """Periodic updater loop."""
        while not self._stop_flag:
            try:
                await self._render()
            except Exception:
                pass
            await asyncio.sleep(3.5)

    def _progress_bar(self, progress: float, width: int = 20) -> str:
        filled = math.floor(progress * width)
        bar = "▓" * filled + "░" * (width - filled)
        percent = f"{progress * 100:5.1f}%"
        return f"{bar} {percent}"

    def _format_speed(self, bytes_per_sec: float) -> str:
        if bytes_per_sec <= 0:
            return "0 KB/s"
        units = ["B/s", "KB/s", "MB/s", "GB/s"]
        power = min(int(math.log(bytes_per_sec, 1024)), len(units) - 1)
        speed = bytes_per_sec / (1024 ** power)
        return f"{speed:.1f} {units[power]}"

    async def _render(self, final: bool = False):
        async with self._lock:
            progress = min(self.processed / self.total if self.total else 0, 1.0)
            remaining = max(self.total - self.processed, 0)
            bar = self._progress_bar(progress)
            text = (
                "╔═📊 **Upload Progress** ═╗\n"
                f"{bar}\n"
                f"✅ **Processed:** {self.processed} / {self.total}\n"
                f"⏳ **Remaining:** {remaining}\n"
                f"⚡ **Download:** {self._format_speed(self.download_speed)} | "
                f"**Upload:** {self._format_speed(self.upload_speed)}\n"
                f"❌ **Failed:** {self.failed}\n"
            )
            if self.next_item:
                text += f"⏭ **Next:** {self.next_item}\n"
            text += "╚═══════════════════════╝"
            if final:
                text += "\n✅ **Completed!**"
            try:
                await self.message.edit_text(text, disable_web_page_preview=True)
            except Exception:
                pass
