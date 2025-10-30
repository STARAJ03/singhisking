# ──────────────────────────────────────────────
#  Base image
# ──────────────────────────────────────────────
FROM python:3.10-slim

# ──────────────────────────────────────────────
#  Environment setup
# ──────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TZ=Asia/Kolkata

# ──────────────────────────────────────────────
#  System dependencies (includes ffmpeg)
# ──────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    ffmpeg \
    libsm6 \
    libxext6 \
    git \
    curl \
    wget \
    tzdata \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*

# ──────────────────────────────────────────────
#  Working directory
# ──────────────────────────────────────────────
WORKDIR /app

# ──────────────────────────────────────────────
#  Copy project files
# ──────────────────────────────────────────────
COPY . /app

# ──────────────────────────────────────────────
#  Python dependencies
# ──────────────────────────────────────────────
RUN pip install --no-cache-dir -U pip setuptools wheel \
 && pip install --no-cache-dir -r requirements.txt

# ──────────────────────────────────────────────
#  Runtime environment variables
# ──────────────────────────────────────────────
# You can also pass BOT_TOKEN as an environment variable in `docker run`
# ENV BOT_TOKEN=""

# ──────────────────────────────────────────────
#  Start command
# ──────────────────────────────────────────────
CMD ["python", "main.py"]
