# logic_resolver.py
"""
Resolve and pre-sign/proxy video URLs so ffmpeg / yt-dlp can fetch them directly.
This mirrors the behaviour in your working uploader: VisionIAS scraping,
Classplus/Testbook signing, Brightcove bcov_auth append, PW proxy, encrypted.m parsing,
and yt-dlp format hints.

It expects `globals.py` to be present with cptoken/cwtoken/pwtoken variables.
"""

import re
import os
import requests
import asyncio
import aiohttp
import logging

logger = logging.getLogger("logic_resolver")
logger.setLevel(logging.INFO)

# Pull tokens from your globals.py (as you requested)

CPTOKEN = "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpZCI6MTYzNjkyNjM0LCJvcmdJZCI6NjA5NzQyLCJ0eXBlIjoxLCJtb2JpbGUiOiI5MTk0MDQ0MDg3NDAiLCJuYW1lIjoiTXlyYSIsImVtYWlsIjoicml5YWhzaHJpdmFzdGF2NCs1MzMyQGdtYWlsLmNvbSIsImlzRmlyc3RMb2dpbiI6dHJ1ZSwiZGVmYXVsdExhbmd1YWdlIjoiRU4iLCJjb3VudHJ5Q29kZSI6IklOIiwiaXNJbnRlcm5hdGlvbmFsIjowLCJpc0RpeSI6dHJ1ZSwibG9naW5WaWEiOiJPdHAiLCJmaW5nZXJwcmludElkIjoiODZmN2RhMjMyMzgxNDk2YTliMjY4YzhkMTAxOGNkMGEiLCJpYXQiOjE3NTkyMTA3ODksImV4cCI6MTc1OTgxNTU4OX0.O3DG_gMpOUet2HKSmH1jK9EEWmjREEMh4cX7DW4yqqkCTzcV5C6-lr6zaY1ihhR4"
CWTOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3MjQyMzg3OTEsImNvbiI6eyJpc0FkbWluIjpmYWxzZSwiYXVzZXIiOiJVMFZ6TkdGU2NuQlZjR3h5TkZwV09FYzBURGxOZHowOSIsImlkIjoiZEUxbmNuZFBNblJqVEROVmFWTlFWbXhRTkhoS2R6MDkiLCJmaXJzdF9uYW1lIjoiYVcxV05ITjVSemR6Vm10ak1WUlBSRkF5ZVNzM1VUMDkiLCJlbWFpbCI6Ik5Ga3hNVWhxUXpRNFJ6VlhiR0ppWTJoUk0wMVdNR0pVTlU5clJXSkRWbXRMTTBSU2FHRnhURTFTUlQwPSIsInBob25lIjoiVUhVMFZrOWFTbmQ1ZVcwd1pqUTViRzVSYVc5aGR6MDkiLCJhdmF0YXIiOiJLM1ZzY1M4elMwcDBRbmxrYms4M1JEbHZla05pVVQwOSIsInJlZmVycmFsX2NvZGUiOiJOalZFYzBkM1IyNTBSM3B3VUZWbVRtbHFRVXAwVVQwOSIsImRldmljZV90eXBlIjoiYW5kcm9pZCIsImRldmljZV92ZXJzaW9uIjoiUShBbmRyb2lkIDEwLjApIiwiZGV2aWNlX21vZGVsIjoiU2Ftc3VuZyBTTS1TOTE4QiIsInJlbW90ZV9hZGRyIjoiNTQuMjI2LjI1NS4xNjMsIDU0LjIyNi4yNTUuMTYzIn19.snDdd-PbaoC42OUhn5SJaEGxq0VzfdzO49WTmYgTx8ra_Lz66GySZykpd2SxIZCnrKR6-R10F5sUSrKATv1CDk9ruj_ltCjEkcRq8mAqAytDcEBp72-W0Z7DtGi8LdnY7Vd9Kpaf499P-y3-godolS_7ixClcYOnWxe2nSVD5C9c5HkyisrHTvf6NFAuQC_FD3TzByldbPVKK0ag1UnHRavX8MtttjshnRhv5gJs5DQWj4Ir_dkMcJ4JaVZO3z8j0OxVLjnmuaRBujT-1pavsr1CCzjTbAcBvdjUfvzEhObWfA1-Vl5Y4bUgRHhl1U-0hne4-5fF0aouyu71Y6W0eg'
PWTOKEN = None

async def resolve_url(url: str, name: str = None, raw_text2: str = "1080") -> dict:
    """
    Returns a dict:
      {
        "url": resolved_url,
        "mpd": mpd_url_or_none,
        "keys": keys_list_or_none,
        "keys_string": keys_cli_string_or_none,
        "appxkey": appxkey_or_none,
        "ytf": yt-dlp format string hint,
        "cmd": optional yt-dlp command suggestion,
        "error": error message or None
      }
    """
    result = {
        "url": url,
        "mpd": None,
        "keys": None,
        "keys_string": None,
        "appxkey": None,
        "ytf": None,
        "cmd": None,
        "error": None
    }

    if not url:
        result["error"] = "Empty URL"
        return result

    url_lower = url.lower()

    # --- VisionIAS: scrape for playlist.m3u8 ---
    if "visionias" in url_lower:
        try:
            headers = {
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Pragma': 'no-cache',
                'Referer': 'http://www.visionias.in/',
                'User-Agent': 'Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36',
            }
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers, timeout=15) as resp:
                    text = await resp.text()
                    m = re.search(r'(https://.*?playlist\.m3u8.*?)["\']', text)
                    if m:
                        result["url"] = m.group(1)
                        return result
        except Exception as e:
            logger.warning(f"visionias parse failed: {e}")

    # --- ACECWPLY (build yt-dlp command) ---
    if "acecwply" in url_lower:
        if name is None:
            name = "output"
        ytf = f'bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]'
        result["ytf"] = ytf
        result["cmd"] = f'yt-dlp -o "{name}.%(ext)s" -f "{ytf}" --hls-prefer-ffmpeg --no-keep-video --remux-video mkv --no-warning "{url}"'
        return result

    # --- cpvod.testbook / classplusapp.com/drm -> convert to media-cdn and sign if possible ---
    if "https://cpvod.testbook.com/" in url_lower or "classplusapp.com/drm/" in url_lower:
        try:
            url_conv = url.replace("https://cpvod.testbook.com/", "https://media-cdn.classplusapp.com/drm/")
            if CPTOKEN:
                headers = {'x-access-token': CPTOKEN}
                params = {"url": url_conv}
                r = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params, timeout=12)
                if r.ok:
                    data = r.json()
                    if data.get("url"):
                        result["mpd"] = data.get("url")
                        if data.get("keys"):
                            result["keys"] = data.get("keys")
                            result["keys_string"] = " ".join([f"--key {k}" for k in data.get("keys")])
                        result["url"] = result["mpd"]
                        return result
        except Exception as e:
            logger.warning(f"cpvod/testbook signing failed: {e}")
            result["error"] = str(e)
            return result

    # --- tencdn / media-cdn / videos.classplusapp: use jw-signed-url endpoint ---
    if any(x in url_lower for x in ["tencdn.classplusapp", "videos.classplusapp", "media-cdn.classplusapp.com", "media-cdn-alisg.classplusapp.com", "media-cdn-a.classplusapp.com"]):
        try:
            headers = {
                'host': 'api.classplusapp.com',
                'x-access-token': CPTOKEN or "",
                'accept-language': 'EN',
                'api-version': '18',
                'app-version': '1.4.73.2',
                'build-number': '35',
                'connection': 'Keep-Alive',
                'content-type': 'application/json',
                'device-details': 'Xiaomi_Redmi 7_SDK-32',
                'device-id': 'c28d3cb16bbdac01',
                'region': 'IN',
                'user-agent': 'Mobile-Android',
                'webengage-luid': '00000187-6fe4-5d41-a530-26186858be4c',
                'accept-encoding': 'gzip'
            }
            params = {"url": url}
            r = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params, timeout=12)
            if r.ok:
                data = r.json()
                if "url" in data:
                    result["url"] = data["url"]
                    return result
        except Exception as e:
            logger.warning(f"classplus signing failed: {e}")

    # --- Brightcove bcov_auth replacement using cwtoken (if present) ---
    # --- Brightcove bcov_auth replacement using cwtoken (if present) ---
    if any(x in url_lower for x in ["edge.api.brightcove.com", "d14v4v80cpjht7.cloudfront.net/"]):
        try:
            if CWTOKEN:
                if "bcov_auth" not in url_lower:
                    joiner = "&" if "?" in url else "?"
                    url = f"{url}{joiner}bcov_auth={CWTOKEN}"
                result["url"] = url
                return result
        except Exception as e:
            logger.warning(f"brightcove handling failed: {e}")


    # --- anonymouspw proxy (childId+parentId) ---
    if "childid" in url_lower and "parentid" in url_lower:
        try:
            if PWTOKEN:
                result["url"] = f"https://anonymouspwplayer-0e5a3f512dec.herokuapp.com/pw?url={url}&token={PWTOKEN}"
                return result
        except Exception as e:
            logger.warning(f"pw proxy handling failed: {e}")

    # --- encrypted.m appxkey handling (format url*key) ---
    if 'encrypted.m' in url_lower:
        try:
            if '*' in url:
                parts = url.split('*')
                result["appxkey"] = parts[1] if len(parts) > 1 else None
                result["url"] = parts[0]
                return result
        except Exception as e:
            logger.warning(f"encrypted.m parsing failed: {e}")

    # --- Determine yt-dlp format hints ---
    try:
        if "youtu" in url_lower:
            ytf = f"bv*[height<={raw_text2}][ext=mp4]+ba[ext=m4a]/b[height<=?{raw_text2}]"
        elif "embed" in url_lower:
            ytf = f"bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]"
        else:
            ytf = f"b[height<={raw_text2}]/bv[height<={raw_text2}]+ba/b/bv+ba"
        result["ytf"] = ytf
    except Exception:
        result["ytf"] = None

    # --- Build default yt-dlp command suggestion ---
    try:
        if name is None:
            name = "output"
        if "jw-prod" in url_lower:
            cmd = f'yt-dlp -o "{name}.mp4" "{url}"'
        elif "webvideos.classplusapp." in url_lower:
            cmd = f'yt-dlp --add-header "referer:https://web.classplusapp.com/" --add-header "x-cdn-tag:empty" -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        elif "youtube.com" in url_lower or "youtu.be" in url_lower:
            cmd = f'yt-dlp --cookies youtube_cookies.txt -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        else:
            cmd = f'yt-dlp -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        result["cmd"] = cmd
    except Exception as e:
        logger.warning(f"yt-dlp command build failed: {e}")

    return result
