# link_resolver.py
"""
Resolve and (if required) pre-sign/proxy video URLs so ffmpeg/yt-dlp can download them.
Returns a dict with keys:
  - url: resolved URL (playable .m3u8/mpd/etc or direct)
  - cmd: optional yt-dlp command string suggestion (None if not applicable)
  - mpd: mpd url when drm -> mpd flow used
  - keys: keys list if drm keys are returned
  - keys_string: string of --key args for ffmpeg if needed
  - appxkey: appxkey for encrypted.m style links (if present)
  - error: error message if resolution failed
"""
import re
import os
import requests
import asyncio
import aiohttp
import logging

# import your runtime tokens / config from globals.py (your repo already has this)
try:
    import globals
    CPTOKEN = getattr(globals, "cptoken", None)
    CWTOKEN = getattr(globals, "cwtoken", None)
    PWTOKEN = getattr(globals, "pwtoken", None)
except Exception:
    CPTOKEN = "eyJhbGciOiJIUzM4NCIsInR5cCI6IkpXVCJ9.eyJpZCI6MTYzNjkyNjM0LCJvcmdJZCI6NjA5NzQyLCJ0eXBlIjoxLCJtb2JpbGUiOiI5MTk0MDQ0MDg3NDAiLCJuYW1lIjoiTXlyYSIsImVtYWlsIjoicml5YWhzaHJpdmFzdGF2NCs1MzMyQGdtYWlsLmNvbSIsImlzRmlyc3RMb2dpbiI6dHJ1ZSwiZGVmYXVsdExhbmd1YWdlIjoiRU4iLCJjb3VudHJ5Q29kZSI6IklOIiwiaXNJbnRlcm5hdGlvbmFsIjowLCJpc0RpeSI6dHJ1ZSwibG9naW5WaWEiOiJPdHAiLCJmaW5nZXJwcmludElkIjoiODZmN2RhMjMyMzgxNDk2YTliMjY4YzhkMTAxOGNkMGEiLCJpYXQiOjE3NTkyMTA3ODksImV4cCI6MTc1OTgxNTU4OX0.O3DG_gMpOUet2HKSmH1jK9EEWmjREEMh4cX7DW4yqqkCTzcV5C6-lr6zaY1ihhR4"
    CWTOKEN = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiJ9.eyJpYXQiOjE3MjQyMzg3OTEsImNvbiI6eyJpc0FkbWluIjpmYWxzZSwiYXVzZXIiOiJVMFZ6TkdGU2NuQlZjR3h5TkZwV09FYzBURGxOZHowOSIsImlkIjoiZEUxbmNuZFBNblJqVEROVmFWTlFWbXhRTkhoS2R6MDkiLCJmaXJzdF9uYW1lIjoiYVcxV05ITjVSemR6Vm10ak1WUlBSRkF5ZVNzM1VUMDkiLCJlbWFpbCI6Ik5Ga3hNVWhxUXpRNFJ6VlhiR0ppWTJoUk0wMVdNR0pVTlU5clJXSkRWbXRMTTBSU2FHRnhURTFTUlQwPSIsInBob25lIjoiVUhVMFZrOWFTbmQ1ZVcwd1pqUTViRzVSYVc5aGR6MDkiLCJhdmF0YXIiOiJLM1ZzY1M4elMwcDBRbmxrYms4M1JEbHZla05pVVQwOSIsInJlZmVycmFsX2NvZGUiOiJOalZFYzBkM1IyNTBSM3B3VUZWbVRtbHFRVXAwVVQwOSIsImRldmljZV90eXBlIjoiYW5kcm9pZCIsImRldmljZV92ZXJzaW9uIjoiUShBbmRyb2lkIDEwLjApIiwiZGV2aWNlX21vZGVsIjoiU2Ftc3VuZyBTTS1TOTE4QiIsInJlbW90ZV9hZGRyIjoiNTQuMjI2LjI1NS4xNjMsIDU0LjIyNi4yNTUuMTYzIn19.snDdd-PbaoC42OUhn5SJaEGxq0VzfdzO49WTmYgTx8ra_Lz66GySZykpd2SxIZCnrKR6-R10F5sUSrKATv1CDk9ruj_ltCjEkcRq8mAqAytDcEBp72-W0Z7DtGi8LdnY7Vd9Kpaf499P-y3-godolS_7ixClcYOnWxe2nSVD5C9c5HkyisrHTvf6NFAuQC_FD3TzByldbPVKK0ag1UnHRavX8MtttjshnRhv5gJs5DQWj4Ir_dkMcJ4JaVZO3z8j0OxVLjnmuaRBujT-1pavsr1CCzjTbAcBvdjUfvzEhObWfA1-Vl5Y4bUgRHhl1U-0hne4-5fF0aouyu71Y6W0eg'
    PWTOKEN = None

logger = logging.getLogger("link_resolver")
logger.setLevel(logging.INFO)


async def resolve_url(url: str, name: str = None, raw_text2: str = "1080") -> dict:
    """
    Normalize/sign the given url. Returns a dictionary with useful fields.
    Keep parameters minimal so main.py can call: resolved = await resolve_url(url, name, raw_text2)
    """
    result = {
        "url": url,
        "cmd": None,
        "mpd": None,
        "keys": None,
        "keys_string": None,
        "appxkey": None,
        "ytf": None,
        "error": None
    }

    if not url:
        result["error"] = "Empty URL"
        return result

    url_lower = url.lower()

    # ---------- VISIONIAS: scrape page and extract playlist.m3u8 ----------
    if "visionias" in url_lower:
        try:
            async with aiohttp.ClientSession() as session:
                headers = {
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Cache-Control': 'no-cache',
                    'Connection': 'keep-alive',
                    'Pragma': 'no-cache',
                    'Referer': 'http://www.visionias.in/',
                    'User-Agent': 'Mozilla/5.0 (Linux; Android 12) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/107.0.0.0 Mobile Safari/537.36',
                }
                async with session.get(url, headers=headers, timeout=15) as resp:
                    text = await resp.text()
                    m = re.search(r'(https://.*?playlist\.m3u8.*?)"', text)
                    if m:
                        result["url"] = m.group(1)
                        return result
        except Exception as e:
            logger.warning(f"visionias parsing failed: {e}")

    # ---------- ACECWPLY special handling (just build yt-dlp cmd if requested) ----------
    if "acecwply" in url_lower:
        # build yt-dlp command suggestion
        if name is None:
            name = "output"
        # choose format that respects requested height
        ytf = f'bestvideo[height<={raw_text2}]+bestaudio/best[height<={raw_text2}]'
        result["ytf"] = ytf
        result["cmd"] = f'yt-dlp -o "{name}.%(ext)s" -f "{ytf}" --hls-prefer-ffmpeg --no-keep-video --remux-video mkv --no-warning "{url}"'
        return result

    # ---------- cpvod.testbook / classplusapp.com/drm -> use existing signer service if available ----------
    if "https://cpvod.testbook.com/" in url_lower or "classplusapp.com/drm/" in url_lower:
        try:
            # convert cpvod -> media-cdn path used by the other uploader
            url = url.replace("https://cpvod.testbook.com/", "https://media-cdn.classplusapp.com/drm/")
            # Example: the other uploader called an external signing endpoint (sainibotsdrm.vercel.app)
            # Here, we try the Classplus JW signed-url API first (same as later block).
            if CPTOKEN:
                headers = {'x-access-token': CPTOKEN}
                params = {"url": url}
                r = requests.get('https://api.classplusapp.com/cams/uploader/video/jw-signed-url', headers=headers, params=params, timeout=12)
                if r.ok and "url" in r.json():
                    data = r.json()
                    result["mpd"] = data.get("url")
                    # If they return keys too, preserve them
                    if data.get("keys"):
                        result["keys"] = data.get("keys")
                        result["keys_string"] = " ".join([f"--key {k}" for k in result["keys"]])
                    # prefer mpd if provided
                    if result["mpd"]:
                        result["url"] = result["mpd"]
                        return result
            # fallback: try external signin service if you have one - placeholder
        except Exception as e:
            logger.warning(f"cpvod/testbook signing failed: {e}")
            result["error"] = str(e)
            return result

    # ---------- tencdn.classplusapp / media-cdn.classplusapp / videos.classplusapp ----------
    if "tencdn.classplusapp" in url_lower or "videos.classplusapp" in url_lower or any(x in url_lower for x in ["media-cdn.classplusapp.com", "media-cdn-alisg.classplusapp.com", "media-cdn-a.classplusapp.com"]):
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

    # ---------- Brightcove edge.api.brightcove.com ----------
    if "edge.api.brightcove.com" in url_lower:
        try:
            # the other script appended bcov_auth token param using cwtoken
            if CWTOKEN:
                # if url already has bcov_auth, skip; otherwise append
                if "bcov_auth" not in url_lower:
                    # naive appendix - keep original query base
                    if "?" in url:
                        url = url.split("bcov_auth")[0] + f"bcov_auth={CWTOKEN}"
                    else:
                        url = url + f"?bcov_auth={CWTOKEN}"
                    result["url"] = url
                    return result
        except Exception as e:
            logger.warning(f"brightcove handling failed: {e}")

    # ---------- anonymouspw / pwtoken handling (childId/parentId) ----------
    if "childid" in url_lower and "parentid" in url_lower:
        try:
            if PWTOKEN:
                url = f"https://anonymouspwplayer-0e5a3f512dec.herokuapp.com/pw?url={url}&token={PWTOKEN}"
                result["url"] = url
                return result
        except Exception as e:
            logger.warning(f"pw proxy handling failed: {e}")

    # ---------- encrypted.m (appxkey) ----------
    if 'encrypted.m' in url_lower:
        try:
            # other uploader used format like url*appxkey
            if '*' in url:
                parts = url.split('*')
                result["appxkey"] = parts[1] if len(parts) > 1 else None
                result["url"] = parts[0]
                return result
        except Exception as e:
            logger.warning(f"encrypted.m parsing failed: {e}")

    # ---------- YT / embed detection -> build ytf format for yt-dlp ----------
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

    # ---------- Decide yt-dlp command fallback ----------
    try:
        if name is None:
            name = "output"
        if "jw-prod" in url_lower:
            cmd = f'yt-dlp -o "{name}.mp4" "{url}"'
        elif "webvideos.classplusapp." in url_lower:
            cmd = f'yt-dlp --add-header "referer:https://web.classplusapp.com/" --add-header "x-cdn-tag:empty" -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        elif "youtube.com" in url_lower or "youtu.be" in url_lower:
            # assumes you manage cookies externally (youtube_cookies.txt)
            cmd = f'yt-dlp --cookies youtube_cookies.txt -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        else:
            cmd = f'yt-dlp -f "{result["ytf"]}" "{url}" -o "{name}.mp4"'
        result["cmd"] = cmd
    except Exception as e:
        logger.warning(f"yt-dlp command build failed: {e}")

    return result
