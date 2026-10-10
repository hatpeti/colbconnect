import asyncio
import os
import websockets
import json
import random
import re
import time
import math
import hashlib
import mimetypes
import glob
import shutil
import contextlib
import subprocess
import html
import logging
from pathlib import Path

import nest_asyncio
import aria2p

from wzgram import Client, filters, enums, raw
from wzgram.handlers import MessageHandler, CallbackQueryHandler
from wzgram.types import InlineKeyboardMarkup, InlineKeyboardButton
from wzgram.errors import FloodWait, RPCError

nest_asyncio.apply()
logging.getLogger("wzgram").setLevel(logging.ERROR)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Reduce wzgram's internal retry count from 10 to 3
# This prevents hammering DC5 with rapid-fire GetFile requests
# Our outer retry loop handles reconnection with fresh file_reference instead
try:
    from wzgram.session import Session
    Session.MAX_RETRIES = 3
    logger.info("wzgram Session.MAX_RETRIES set to 3")
except Exception:
    pass

MASTER_WS_URL = "wss://worker-production-d47f.up.railway.app"
TARGET_CHANNEL = "@animedubsinhla"
WATERMARK = "@animesinhala1"
PRIVATE_DB_CHANNEL = -1004327562659  # Private Channel for 4K chunk auto-save/resume

app = None
user_app = None
upload_client = None
aria2_api = None

DOWNLOAD_MAX_RETRIES = 8
UPLOAD_MAX_RETRIES = 5

# Settings & Concurrency
CONCURRENCY_LIMIT = 4
TASK_SEMAPHORE = asyncio.Semaphore(CONCURRENCY_LIMIT)
PART_SIZE = 512 * 1024
UPLOAD_WORKERS = 8
PART_RETRIES = 8
UI_INTERVAL = 3.0
MAX_FILE_SIZE = 2000 * 1024 * 1024
BIG_FILE_THRESHOLD = 10 * 1024 * 1024
SESSION_DIR = "/content/telegram_sessions"
os.makedirs(SESSION_DIR, exist_ok=True)
THUMB_DIR = "/content/bot_thumbnail"
os.makedirs(THUMB_DIR, exist_ok=True)
CUSTOM_THUMB_PATH = os.path.join(THUMB_DIR, "custom_thumb.jpg")

# Global State Tracker
ACTIVE_TASKS = {}
STATUS_MESSAGES = {}
cancel_flags = {}
upload_runtime = {}
current_tasks = {}
pending_sub_replies = {}
pending_audio_replies = {}
pending_renames = {}

_prev_cpu_times = [0.0, 0.0]

# --- SYSTEM STATS & FORMATTING HELPERS ---
def get_system_stats():
    global _prev_cpu_times
    cpu_pct = 0.0
    try:
        with open("/proc/stat", "r") as f:
            fields = [float(x) for x in f.readline().split()[1:8]]
        idle = fields[3] + fields[4]
        total = sum(fields)
        idle_delta = idle - _prev_cpu_times[0]
        total_delta = total - _prev_cpu_times[1]
        _prev_cpu_times = [idle, total]
        if total_delta > 0:
            cpu_pct = round(100.0 * (1.0 - idle_delta / total_delta), 1)
    except:
        cpu_pct = 0.0

    ram_pct = 0.0
    try:
        mem = {}
        with open("/proc/meminfo", "r") as f:
            for line in f:
                parts = line.split(":")
                if len(parts) == 2:
                    mem[parts[0].strip()] = float(parts[1].split()[0])
        total_mem = mem.get("MemTotal", 1.0)
        avail_mem = mem.get("MemAvailable", mem.get("MemFree", 0.0))
        used_mem = total_mem - avail_mem
        ram_pct = round((used_mem / total_mem) * 100, 1)
    except:
        ram_pct = 0.0

    disk_free_str, disk_pct = "0 GB", 0.0
    try:
        path = "/content" if os.path.exists("/content") else "."
        total_b, used_b, free_b = shutil.disk_usage(path)
        disk_pct = round((used_b / total_b) * 100, 1)
        disk_free_str = format_bytes(free_b)
    except:
        pass

    uptime_str = "0m"
    try:
        with open("/proc/uptime", "r") as f:
            up_secs = float(f.read().split()[0])
        d = int(up_secs // 86400)
        h = int((up_secs % 86400) // 3600)
        m = int((up_secs % 3600) // 60)
        if d > 0: uptime_str = f"{d}d {h}h {m}m"
        elif h > 0: uptime_str = f"{h}h {m}m"
        else: uptime_str = f"{m}m"
    except:
        uptime_str = "N/A"

    return cpu_pct, disk_free_str, disk_pct, ram_pct, uptime_str

def check_gpu():
    try: return subprocess.run(["nvidia-smi"], capture_output=True, text=True).returncode == 0
    except: return False

def format_bytes(size):
    try: size = float(size)
    except: size = 0.0
    if size <= 0: return "0 B"
    units = ("B", "KB", "MB", "GB", "TB", "PB")
    i = min(int(math.floor(math.log(size, 1024))), len(units) - 1)
    value = size / (1024 ** i)
    if value >= 10: return f"{value:.1f} {units[i]}"
    return f"{value:.2f} {units[i]}"

def format_mbps(bytes_per_second):
    return f"{(bytes_per_second * 8) / 1_000_000:.2f} Mbps"

def format_time(seconds):
    try: seconds = max(0, int(seconds))
    except: seconds = 0
    if seconds == 0: return "0s"
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    if h > 0: return f"{h}h {m}m {s}s"
    if m > 0: return f"{m}m {s}s"
    return f"{s}s"

def make_progress_bar(percentage):
    pct = max(0.0, min(100.0, percentage))
    filled = int(round(pct / 10))
    bar = "■" * filled + "□" * (10 - filled)
    return bar, pct

def safe_html(text): return html.escape(str(text), quote=False)

def get_mime_type(path): return mimetypes.guess_type(path)[0] or "application/octet-stream"

# --- RETRY HELPERS FOR DOWNLOAD & UPLOAD ---
async def download_with_retry(client, message, file_name, chat_id=None, msg_id=None, progress=None, max_retries=DOWNLOAD_MAX_RETRIES):
    """Download a Telegram file with retry logic.
    
    Key fix: On each retry attempt:
    1. Restart the client session (force new DC connection)
    2. Re-fetch the message via get_messages() for fresh file_reference
    3. Wait with exponential backoff for Telegram servers to recover
    
    file_reference expires quickly on Telegram. If we keep retrying with the
    same stale reference, every attempt will fail with -503 Timeout.
    """
    last_error = None
    current_msg = message
    
    # Log file info for debugging
    media = message.document or message.video
    if media:
        dc_id = getattr(media, "dc_id", "?")
        file_size = getattr(media, "file_size", 0)
        file_ref = getattr(media, "file_reference", None)
        ref_hex = file_ref[:8].hex() + "..." if file_ref and len(file_ref) > 8 else str(file_ref)
        logger.info(f"📋 File info: DC={dc_id}, size={format_bytes(file_size)}, ref={ref_hex}")
    
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Download attempt {attempt}/{max_retries} for {os.path.basename(file_name)}")
            
            # On retry (attempt > 1): restart session + re-fetch message for fresh file_reference
            if attempt > 1:
                # Send diagnostic to chat so user can see progress
                if chat_id:
                    with contextlib.suppress(Exception):
                        await client.send_message(
                            chat_id,
                            f"🔄 <b>Download retry {attempt}/{max_retries}</b>\n"
                            f"├ Refreshing connection & file reference...\n"
                            f"└ Previous error: <code>{safe_html(str(last_error)[:150])}</code>",
                            parse_mode=enums.ParseMode.HTML
                        )
                
                # 1. Restart client to force new DC connection
                try:
                    if client.is_connected:
                        logger.info("Restarting client session for fresh connection...")
                        await client.restart()
                    else:
                        logger.info("Client disconnected, starting fresh...")
                        await client.start()
                    logger.info("Client session restarted successfully")
                except Exception as rc_err:
                    logger.warning(f"Session restart failed: {rc_err}, trying start()...")
                    try:
                        await client.start()
                    except Exception:
                        pass
                
                # 2. Re-fetch message for fresh file_reference
                if chat_id and msg_id:
                    try:
                        fresh_msg = await client.get_messages(chat_id, msg_id)
                        if fresh_msg and (fresh_msg.document or fresh_msg.video):
                            current_msg = fresh_msg
                            fresh_media = fresh_msg.document or fresh_msg.video
                            new_ref = getattr(fresh_media, "file_reference", None)
                            new_hex = new_ref[:8].hex() + "..." if new_ref and len(new_ref) > 8 else "None"
                            logger.info(f"✅ Fresh file_reference: {new_hex}")
                        else:
                            logger.warning(f"Re-fetched message {msg_id} but no media found")
                    except Exception as gm_err:
                        logger.warning(f"Failed to re-fetch message: {gm_err}")
                
                # 3. Clean up any partial download from previous attempt
                if os.path.exists(file_name):
                    with contextlib.suppress(Exception): os.remove(file_name)
            else:
                # First attempt: just check connection
                if not client.is_connected:
                    logger.warning("Client disconnected, reconnecting...")
                    try:
                        await client.start()
                    except Exception as rc_err:
                        logger.warning(f"Reconnect failed: {rc_err}")
            
            path = await client.download_media(current_msg, file_name=file_name, progress=progress)
            if path and os.path.exists(path):
                file_size = os.path.getsize(path)
                expected_size = getattr(current_msg.document or current_msg.video, "file_size", 0)
                if expected_size > 0 and file_size < expected_size * 0.95:
                    logger.warning(f"Incomplete download: {file_size} / {expected_size} bytes")
                    with contextlib.suppress(Exception): os.remove(path)
                    raise Exception(f"Incomplete download: got {format_bytes(file_size)}, expected {format_bytes(expected_size)}")
                logger.info(f"✅ Download successful: {path} ({format_bytes(file_size)})")
                return path
            raise Exception("Download returned None or file does not exist")
        except asyncio.CancelledError:
            raise
        except FloodWait as fw:
            wait = min(int(getattr(fw, "value", 5)), 120)
            logger.warning(f"FloodWait during download: sleeping {wait}s")
            await asyncio.sleep(wait)
        except Exception as e:
            last_error = e
            logger.warning(f"�?� Download attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                break
            # Longer backoff to let DC5 cooldown: 30s, 60s, 120s, 180s, 180s...
            wait_time = min(30 * (2 ** (attempt - 1)), 180)
            logger.info(f"�?� Waiting {wait_time}s before retry (fresh session + file_reference)...")
            await asyncio.sleep(wait_time)
    raise Exception(f"Download failed after {max_retries} attempts. Last error: {last_error}")

async def upload_with_retry(client, chat_id, f_path, f_name, thumb, progress=None, max_retries=UPLOAD_MAX_RETRIES):
    """Upload a file to Telegram with retry logic."""
    last_error = None
    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Upload attempt {attempt}/{max_retries} for {f_name}")
            msg = await client.send_document(
                chat_id=chat_id,
                document=f_path,
                thumb=thumb,
                caption=f"<code>{f_name}</code>",
                force_document=True,
                progress=progress
            )
            logger.info(f"Upload successful: {f_name}")
            return msg
        except asyncio.CancelledError:
            raise
        except FloodWait as fw:
            wait = min(int(getattr(fw, "value", 5)), 60)
            logger.warning(f"FloodWait during upload: sleeping {wait}s")
            await asyncio.sleep(wait)
            # Don't count FloodWait as a failed attempt
        except Exception as e:
            last_error = e
            logger.warning(f"Upload attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                break
            wait_time = min(2 ** attempt * 3, 60)
            logger.info(f"Retrying upload in {wait_time}s...")
            await asyncio.sleep(wait_time)
    raise Exception(f"Upload failed after {max_retries} attempts. Last error: {last_error}")

def parse_selection(selection_str, max_idx):
    if selection_str.lower() == "all": return list(range(1, max_idx + 1))
    indices = []
    for part in selection_str.split(","):
        part = part.strip()
        if "-" in part:
            try:
                start, end = part.split("-", 1)
                start, end = int(start.strip()), int(end.strip())
                step = 1 if start <= end else -1
                for idx in range(start, end + step, step):
                    if 1 <= idx <= max_idx and idx not in indices:
                        indices.append(idx)
            except: pass
        elif part.isdigit():
            idx = int(part)
            if 1 <= idx <= max_idx and idx not in indices:
                indices.append(idx)
    return indices

def build_file_tree(files, is_html=True):
    tree = {}
    for i, f in enumerate(files):
        parts = Path(f.path).parts
        current = tree
        for i_part, part in enumerate(parts):
            if i_part == len(parts) - 1:
                current[part] = {'index': i + 1, 'size': f.length}
            else:
                if part not in current: current[part] = {}
                current = current[part]
    def render_tree(node, prefix=""):
        lines = []
        keys = list(node.keys())
        for idx, key in enumerate(keys):
            is_last_item = (idx == len(keys) - 1)
            connector = "└── " if is_last_item else "├── "
            child_prefix = "    " if is_last_item else "│   "
            val = node[key]
            if isinstance(val, dict) and 'index' not in val:
                lines.append(f"{prefix}{connector}�? {html.escape(key) if is_html else key}")
                lines.extend(render_tree(val, prefix + child_prefix))
            else:
                idx_str = f"[{val['index']}]"
                if is_html: idx_str = f"<code>{idx_str}</code>"
                lines.append(f"{prefix}{connector}📄 {idx_str} {html.escape(key) if is_html else key} ({val['size'] / (1024*1024):.2f} MB)")
        return lines
    return "\n".join(render_tree(tree))

# --- GLOBAL PROGRESS UI LOOP ---
async def ensure_status_message(chat_id):
    if chat_id in STATUS_MESSAGES:
        return STATUS_MESSAGES[chat_id]
    try:
        msg = await app.send_message(chat_id, "�?� <b>Starting task...</b>", parse_mode=enums.ParseMode.HTML)
        STATUS_MESSAGES[chat_id] = msg
        return msg
    except Exception as e:
        logger.error(f"Error creating status message: {e}")
        return None

async def global_ui_loop():
    while True:
        await asyncio.sleep(UI_INTERVAL)
        if not app or not app.is_connected:
            continue

        active_chats = set(task["chat_id"] for task in ACTIVE_TASKS.values())
        
        # Check registered status messages
        for chat_id in list(STATUS_MESSAGES.keys()):
            status_msg = STATUS_MESSAGES.get(chat_id)
            if not status_msg: continue
            
            chat_tasks = [t for t in ACTIVE_TASKS.values() if t["chat_id"] == chat_id]
            if not chat_tasks:
                try:
                    await status_msg.edit_text("✅ <b>All tasks completed!</b>", parse_mode=enums.ParseMode.HTML)
                except: pass
                STATUS_MESSAGES.pop(chat_id, None)
                continue

            # Render Global Progress Message
            cpu_pct, disk_free, disk_pct, ram_pct, uptime = get_system_stats()
            task_blocks = []

            for idx, t in enumerate(chat_tasks, 1):
                total = max(t.get("total", 1), 1)
                current = max(0, t.get("current", 0))
                pct = (current / total) * 100
                bar, pct_clamped = make_progress_bar(pct)

                if t.get("is_time", False):
                    processed_str = f"{format_time(current)} of {format_time(total)}"
                    speed_str = f"{t.get('speed', 0.0):.2f}x"
                else:
                    processed_str = f"{format_bytes(current)} of {format_bytes(total)}"
                    speed_str = f"{format_bytes(t.get('speed', 0.0))}/s"

                eta_str = format_time(t.get("eta", 0))
                user_disp = t.get("user_mention", "User")
                task_id = t.get("task_id", "")
                fname = t.get("filename", "Unknown")

                block = (
                    f"Task #{idx} By 👤 {user_disp}\n"
                    f"├ 🎬 <b>{safe_html(fname)}</b>\n"
                    f"├ [{bar}] {pct_clamped:.2f}%\n"
                    f"├ Processed → {processed_str}\n"
                    f"├ Status → {t.get('status', 'Processing')}\n"
                    f"├ Speed → {speed_str}\n"
                    f"├ Time → {eta_str}\n"
                    f"└ Stop → /cancel_{task_id}"
                )
                task_blocks.append(block)

            stats_block = (
                f"Bot Stats 🤖\n"
                f"├ CPU → {cpu_pct}% | Disk → {disk_free} [{disk_pct}%]\n"
                f"└ RAM → ram_pct: {ram_pct}% | UP → {uptime}"
            )
            # Fix stats line display
            stats_block = (
                f"<b>Bot Stats</b> 🤖\n"
                f"├ CPU → {cpu_pct}% | Disk → {disk_free} [{disk_pct}%]\n"
                f"└ RAM → {ram_pct}% | UP → {uptime}"
            )

            full_text = "\n\n".join(task_blocks) + "\n\n" + stats_block

            try:
                await status_msg.edit_text(full_text, parse_mode=enums.ParseMode.HTML)
            except FloodWait as fw:
                await asyncio.sleep(min(int(getattr(fw, "value", 2)), 30))
            except Exception as e:
                pass

# --- MEDIA PROCESSING & WATERMARKING ---
async def probe_media_streams(filepath):
    cmd = ["ffprobe", "-v", "error", "-print_format", "json", "-show_streams", "-show_format", filepath]
    proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=60)
    return json.loads(stdout.decode("utf-8", errors="ignore"))

async def get_video_meta(filepath):
    try:
        info = await probe_media_streams(filepath)
        duration = int(float(info.get("format", {}).get("duration", 1) or 1))
        width, height = 1280, 720
        for stream in info.get("streams", []):
            if stream.get("codec_type") == "video":
                width = int(stream.get("width", 1280) or 1280)
                height = int(stream.get("height", 720) or 720)
                break
        return max(duration, 1), max(width, 1), max(height, 1)
    except: return 1, 1280, 720

async def get_thumbnail(filepath):
    thumb_path = filepath + "_thumb.jpg"
    try:
        cmd = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y", "-ss", "00:00:02", "-i", filepath, "-vf", "scale='min(320,iw)':-2", "-vframes", "1", "-q:v", "5", thumb_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await asyncio.wait_for(proc.communicate(), timeout=60)
        if os.path.exists(thumb_path) and os.path.getsize(thumb_path) > 0: return thumb_path
    except: pass
    return None

async def apply_mkv_watermark(filepath, watermark=WATERMARK):
    if not filepath.lower().endswith(".mkv"): return filepath
    try:
        probe = await probe_media_streams(filepath)
        cmd = ["mkvpropedit", filepath, "--edit", "info", "--set", f"title={watermark}"]
        v_c, a_c, s_c = 0, 0, 0
        for stream in probe.get("streams", []):
            ctype = stream.get("codec_type")
            lang = stream.get("tags", {}).get("language", "")
            name = f"{watermark} [{lang.upper()}]" if lang else watermark
            if ctype == "video":
                v_c += 1
                cmd.extend(["--edit", f"track:v{v_c}", "--set", f"name={watermark}"])
            elif ctype == "audio":
                a_c += 1
                cmd.extend(["--edit", f"track:a{a_c}", "--set", f"name={name}"])
            elif ctype == "subtitle":
                s_c += 1
                cmd.extend(["--edit", f"track:s{s_c}", "--set", f"name={name}"])
        proc = await asyncio.create_subprocess_exec(*cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        await proc.communicate()
    except Exception as e:
        logger.error(f"Watermark error: {e}")
    return filepath

def generate_res_name(original_name, target_res):
    name_without_ext = os.path.splitext(original_name)[0]
    def res_repl(m): return f"{target_res}P" if 'P' in m.group(0) else f"{target_res}p"
    enc_base_name = re.sub(r'(?i)(1080p|720p|480p|2160p|4k|1080)', res_repl, name_without_ext)
    if enc_base_name == name_without_ext: enc_base_name += f"_{target_res}p"
    return enc_base_name

# --- FFMPEG & ENCODING OPERATIONS ---
async def run_ffmpeg_operation(cmd, input_path, output_path, total_duration, task_id, action_name, filename):
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    start_time = time.time()
    
    if task_id in ACTIVE_TASKS:
        ACTIVE_TASKS[task_id].update({
            "status": action_name,
            "filename": filename,
            "total": total_duration,
            "is_time": True,
            "start_time": start_time
        })

    while True:
        if cancel_flags.get(task_id):
            process.terminate()
            raise asyncio.CancelledError("Operation cancelled.")
        line = await process.stdout.readline()
        if not line: break
        line = line.decode("utf-8").strip()
        if line.startswith("out_time_us="):
            try:
                time_us = int(line.split("=")[1])
                curr_sec = max(0, time_us / 1_000_000)
                now = time.time()
                elapsed = max(now - start_time, 0.001)
                speed = curr_sec / elapsed
                eta = max(total_duration - curr_sec, 0) / speed if speed > 0 else 0
                if task_id in ACTIVE_TASKS:
                    ACTIVE_TASKS[task_id].update({
                        "current": curr_sec,
                        "speed": speed,
                        "eta": eta
                    })
            except: pass
    await process.wait()
    if process.returncode != 0:
        err = await process.stderr.read()
        raise Exception(f"FFMPEG Failed:\n{err.decode('utf-8', errors='ignore')[-1000:]}")
    return os.path.exists(output_path)

async def encode_video(input_path, output_path, resolution, total_duration, task_id, filename, hevc_10bit=False):
    has_nvenc = check_gpu()
    if resolution == 2160: scale, cq, crf = "scale=-2:'min(2160,ih)'", "26", "24"
    elif resolution == 1080: scale, cq, crf = "scale=-2:'min(1080,ih)'", "30", "28"
    elif resolution == 720: scale, cq, crf = "scale=-2:'min(720,ih)'", "34", "32"
    else: scale, cq, crf = "scale=-2:'min(480,ih)'", "38", "36"

    if hevc_10bit:
        if has_nvenc:
            vcodec = ["-c:v", "hevc_nvenc", "-preset", "p6", "-tune", "hq", "-qp", cq, "-pix_fmt", "p010le"]
            action = f"⚙️ Encoding {resolution}p HEVC 10b (GPU)"
        else:
            vcodec = ["-c:v", "libx265", "-preset", "veryfast", "-crf", crf, "-pix_fmt", "yuv420p10le"]
            action = f"⚙️ Encoding {resolution}p HEVC 10b (CPU)"
    else:
        if has_nvenc:
            vcodec = ["-c:v", "h264_nvenc", "-preset", "p6", "-tune", "hq", "-qp", cq, "-pix_fmt", "yuv420p"]
            action = f"⚙️ Encoding {resolution}p (GPU)"
        else:
            vcodec = ["-c:v", "libx264", "-preset", "veryfast", "-crf", crf, "-pix_fmt", "yuv420p"]
            action = f"⚙️ Encoding {resolution}p (CPU)"
    cmd = ["ffmpeg", "-y", "-hwaccel", "auto", "-i", input_path, "-vf", scale, *vcodec, "-c:a", "copy", "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-map", "0:t?", "-map_chapters", "0", "-c:s", "copy", "-progress", "pipe:1", "-nostats", "-loglevel", "error", output_path]
    return await run_ffmpeg_operation(cmd, input_path, output_path, total_duration, task_id, action, filename)

# --- UI MENUS ---
def get_panel_markup(task_id):
    def btn(text, data, style=enums.ButtonStyle.DEFAULT):
        return InlineKeyboardButton(text, callback_data=data, style=style)

    return InlineKeyboardMarkup([
        [btn("480p", f"panel_480_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("720p", f"panel_720_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("1080p", f"panel_1080_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("2160p", f"panel_2160_{task_id}", style=enums.ButtonStyle.PRIMARY)],
        [btn("480p HEVC 10b", f"panel_480hevc_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("720p HEVC 10b", f"panel_720hevc_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("1080p HEVC 10b", f"panel_1080hevc_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("2160p HEVC 10b", f"panel_2160hevc_{task_id}", style=enums.ButtonStyle.PRIMARY)],
        [btn("🔮 4K Enhance", f"panel_enhance4k_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("✂�? Remove Sub", f"panel_removesub_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("�? Add Sub", f"panel_addsub_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("�? Extract Sub", f"panel_extract_sub_{task_id}", style=enums.ButtonStyle.DEFAULT)],
        [btn("🎵 Extract Audio", f"panel_extract_audio_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("�?�? Rename", f"panel_rename_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("🔄 Re-encode All", f"panel_reencode_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("🔄 Re-encode HEVC 10b", f"panel_reencodehevc_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("🚀 Upload Now", f"panel_upload_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("�?� Cancel", f"panel_cancel_{task_id}", style=enums.ButtonStyle.DANGER)]
    ])

# Sinhala Help Menu
async def testdb_cmd(client, message):
    try:
        await client.send_message(PRIVATE_DB_CHANNEL, "✅ <b>Test Message:</b> DB Channel Connection is Working!", parse_mode=enums.ParseMode.HTML)
        await message.reply_text("✅ Message sent to DB Channel successfully!")
    except Exception as e:
        await message.reply_text(f"�?� Failed to send to DB Channel: {e}")

async def help_cmd(client, message):
    text = (
        "<b>📚 Super Encoder & Leech Bot Help Menu</b>\n\n"
        "<b>📥 ටොරන්ට් භ�?ගත කිරීම:</b>\n"
        "<code>/leech &lt;magnet_link&gt;</code> - Torrent එකක් භ�?ගත කර File list එක ලබ�? ගෙන අව�?්�?ය files ත�?ර�?ග�?නීමට.\n\n"
        "<b>🎨 Encoding Panel Commands:</b>\n"
        "පහත Commands ඔය�?ට Panel එකේ බොත්තම් විදියට වගේම, කෙලින්ම <b>වීඩිය�? එකකට Reply කරලත්</b> ප�?විච්චි කරන්න පුළුවන්.\n\n"
        "<b>🎬 Video Resolutions:</b>\n"
        "<code>/480</code> - 480p වලට Convert කිරීම.\n"
        "<code>/720</code> - 720p වලට Convert කිරීම.\n"
        "<code>/1080</code> - 1080p වලට Convert කිරීම.\n"
        "<code>/reencode</code> - 480p, 720p, 1080p සහ Original එකත් එක්ක File 4ක්ම ලබ�? දීම.\n\n"
        "<b>✂�? Subtitles:</b>\n"
        "<code>/removesub</code> - Soft subtitles අයින් කිරීම.\n"
        "<code>/addsub</code> - Subtitle එකතු කිරීම (වීඩිය�? එකකට subtitle file එකක් reply කරන්න).\n"
        "<code>/extract_sub</code> - Subtitle එක වෙනම ගලව�?ග�?නීම.\n\n"
        "<b>🎵 Audio:</b>\n"
        "<code>/addaudio</code> - අලුත් Audio Track එකක් ද�?මීම.\n"
        "<code>/extract_audio</code> - Audio එක වෙනම ගලව�?ග�?නීම.\n"
        "<code>/remaudio</code> - Audio Track එක අයින් කිරීම.\n\n"
        "<b>🖼 Thumbnail:</b>\n"
        "<code>/extract_thumb</code> - වීඩිය�? එකේ Thumbnail එක ගලව�?ග�?නීම.\n\n"
        "<b>🛑 Tasks න�?ව�?ත්වීම:</b>\n"
        "<code>/cancel_&lt;task_id&gt;</code> - ඕනෑම ක්�?රිය�?වලියක් නතර කිරීමට."
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# --- PROCESS MEDIA PIPELINE (CONCURRENCY CONTROLLED) ---
async def process_media_file(client, chat_id, filepath, action, custom_renames, file_index, task_id, sub_path=None, audio_path=None):
    filename = os.path.basename(filepath)
    if file_index in custom_renames:
        new_filename = custom_renames[file_index]
        new_filepath = os.path.join(os.path.dirname(filepath), new_filename)
        os.rename(filepath, new_filepath)
        filepath = new_filepath
        filename = new_filename
    
    # 1. Apply MKV Watermark
    filepath = await apply_mkv_watermark(filepath)
    duration, w, h = await get_video_meta(filepath)
    
    files_to_upload = []
    
    # Generate requested qualities based on action
    hevc_10bit = "hevc" in action
    base_action = action.replace("hevc", "")
    
    if base_action in ["480", "720", "1080", "2160", "reencode"]:
        res_list = [int(base_action)] if base_action != "reencode" else [480, 720, 1080]
        if base_action == "reencode": files_to_upload.append(filepath) # Keep original
        
        for res in res_list:
            if cancel_flags.get(task_id): break
            out_file = generate_res_name(filename, res) + ("_HEVC10.mkv" if hevc_10bit else ".mkv")
            out_path = os.path.join(os.path.dirname(filepath), out_file)
            success = await encode_video(filepath, out_path, res, duration, task_id, out_file, hevc_10bit=hevc_10bit)
            if success:
                out_path = await apply_mkv_watermark(out_path)
                files_to_upload.append(out_path)
    elif action == "removesub":
        out_path = os.path.join(os.path.dirname(filepath), "nosub_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-map", "0:v", "-map", "0:a?", "-c", "copy", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "✂�? Removing Subs", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    elif action == "addsub" and sub_path:
        out_path = os.path.join(os.path.dirname(filepath), "sub_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-i", sub_path, "-c", "copy", "-c:s", "srt", "-metadata:s:s:0", f"title={WATERMARK}", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "�? Adding Subtitle", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    elif action == "extract_sub":
        import json
        proc = await asyncio.create_subprocess_exec("mkvmerge", "-J", filepath, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        stdout, _ = await proc.communicate()
        sub_tracks = []
        try:
            info = json.loads(stdout)
            for track in info.get("tracks", []):
                if track.get("type") == "subtitles":
                    codec = track.get("codec", "").lower()
                    ext = ".srt" if "subrip" in codec else ".ass" if "substation" in codec else ".sup" if "pgs" in codec else ".vobsub" if "vobsub" in codec else ".srt"
                    sub_tracks.append((track["id"], ext, track.get("properties", {}).get("language", "und")))
        except Exception as e:
            logger.error(f"Error parsing mkvmerge: {e}")
            
        if not sub_tracks:
            await client.send_message(chat_id, f"⚠�? <b>No subtitles found</b> or file is not MKV: <code>{filename}</code>", parse_mode=enums.ParseMode.HTML)
        else:
            base_name = os.path.splitext(filename)[0]
            extract_args = []
            extracted_files = []
            for i, (tid, ext, lang) in enumerate(sub_tracks):
                out_path = os.path.join(os.path.dirname(filepath), f"{base_name}_track{tid}_{lang}{ext}")
                extract_args.extend([f"{tid}:{out_path}"])
                extracted_files.append(out_path)
                
            cmd = ["mkvextract", "tracks", filepath] + extract_args
            ex_proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await ex_proc.communicate()
            
            for out_path in extracted_files:
                if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
                    files_to_upload.append(out_path)
                    
            if not files_to_upload:
                await client.send_message(chat_id, f"⚠�? <b>Failed to extract subtitles</b> from: <code>{filename}</code>", parse_mode=enums.ParseMode.HTML)
    elif action == "addaudio" and audio_path:
        out_path = os.path.join(os.path.dirname(filepath), "audio_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-i", audio_path, "-c", "copy", "-map", "0:v", "-map", "0:a?", "-map", "1:a", "-map", "0:s?", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "🎵 Adding Audio", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    elif action == "extract_audio":
        out_path = os.path.join(os.path.dirname(filepath), os.path.splitext(filename)[0] + ".aac")
        cmd = ["ffmpeg", "-y", "-i", filepath, "-vn", "-map", "0:a:0", "-c:a", "copy", out_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        if os.path.exists(out_path): files_to_upload.append(out_path)
    elif action == "remaudio":
        out_path = os.path.join(os.path.dirname(filepath), "noaudio_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-map", "0:v", "-map", "0:s?", "-c", "copy", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "🔇 Removing Audio", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    elif action == "extract_thumb":
        out_path = os.path.join(os.path.dirname(filepath), os.path.splitext(filename)[0] + ".jpg")
        cmd = ["ffmpeg", "-y", "-ss", "00:00:02", "-i", filepath, "-vframes", "1", out_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        if os.path.exists(out_path): files_to_upload.append(out_path)
    else:
        # Default upload (Upload Now)
        files_to_upload.append(filepath)

    # 2. Upload generated files as Documents (Force File) using Native Fast Upload
    final_files_to_upload = []
    for f_path in files_to_upload:
        if os.path.exists(f_path) and os.path.getsize(f_path) > MAX_FILE_SIZE:
            import glob
            out_dir = os.path.dirname(f_path)
            name, ext = os.path.splitext(os.path.basename(f_path))
            split_pattern = os.path.join(out_dir, f"{name}_pt%02d.mkv")
            cmd = ["mkvmerge", "-o", split_pattern, "--split", "size:1950M", f_path]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await proc.communicate()
            parts = sorted(glob.glob(os.path.join(out_dir, f"{name}_pt*.mkv")))
            if parts:
                final_files_to_upload.extend(parts)
            else:
                final_files_to_upload.append(f_path)
        else:
            final_files_to_upload.append(f_path)

    for f_path in final_files_to_upload:
        if cancel_flags.get(task_id): break
        f_name = os.path.basename(f_path)
        f_size = os.path.getsize(f_path)
        
        start_time = time.time()
        if task_id in ACTIVE_TASKS:
            ACTIVE_TASKS[task_id].update({
                "status": "📤 Uploading Document",
                "filename": f_name,
                "current": 0,
                "total": f_size,
                "is_time": False,
                "start_time": start_time,
                "speed": 0,
                "eta": 0
            })
            
        async def upload_progress(current, total):
            if cancel_flags.get(task_id):
                raise asyncio.CancelledError("Upload cancelled")
            now = time.time()
            elapsed = max(now - start_time, 0.001)
            speed = current / elapsed
            eta = max(total - current, 0) / speed if speed > 0 else 0
            if task_id in ACTIVE_TASKS:
                ACTIVE_TASKS[task_id].update({
                    "current": current,
                    "total": total,
                    "speed": speed,
                    "eta": eta
                })

        try:
            thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
            msg = await upload_with_retry(client, chat_id, f_path, f_name, thumb, progress=upload_progress)
            
            # Automatically copy to Database Channel
            if str(chat_id).lower() != TARGET_CHANNEL.lower() and msg:
                try:
                    await msg.copy(
                        chat_id=TARGET_CHANNEL,
                        caption=f"<code>{f_name}</code>",
                        parse_mode=enums.ParseMode.HTML
                    )
                except Exception as e:
                    logger.error(f"Error copying to channel {TARGET_CHANNEL}: {e}")
                    if "CHAT_WRITE_FORBIDDEN" in str(e) or "CHANNEL_PRIVATE" in str(e):
                        with contextlib.suppress(Exception):
                            await client.send_message(
                                chat_id,
                                f"⚠�? <b>Database Warning:</b> Bot cannot post to <code>{TARGET_CHANNEL}</code>.\n"
                                f"Please add the bot as an <b>Admin with 'Post Messages' permission</b> to the channel!",
                                parse_mode=enums.ParseMode.HTML
                            )
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Upload error for {f_name}: {e}")
            with contextlib.suppress(Exception):
                await client.send_message(
                    chat_id,
                    f"�?� <b>Upload Failed:</b> <code>{safe_html(f_name)}</code>\n"
                    f"Error: <code>{safe_html(str(e)[:200])}</code>\n"
                    f"🔄 Try sending the file again.",
                    parse_mode=enums.ParseMode.HTML
                )

# --- EXECUTE TASK WORKER WITH SEMAPHORE ---
async def execute_task_worker(client, chat_id, task_id, action, media_msg=None, is_telegram_file=False, sub_path=None, audio_path=None, custom_renames=None):
    if custom_renames is None: custom_renames = {}
    async with TASK_SEMAPHORE:
        try:
            cancel_flags[task_id] = False
            await ensure_status_message(chat_id)

            if is_telegram_file and media_msg:
                dl_dir = f"/content/dl_{task_id}"
                os.makedirs(dl_dir, exist_ok=True)
                file_name = getattr(media_msg.document or media_msg.video, "file_name", "downloaded.mkv")
                path = os.path.join(dl_dir, file_name)

                ACTIVE_TASKS[task_id] = {
                    "task_id": task_id,
                    "chat_id": chat_id,
                    "user_mention": getattr(media_msg.from_user, "mention", "User") if media_msg.from_user else "User",
                    "filename": file_name,
                    "status": "📥 Downloading TG File",
                    "current": 0,
                    "total": getattr(media_msg.document or media_msg.video, "file_size", 100),
                    "is_time": False,
                    "start_time": time.time(),
                    "speed": 0,
                    "eta": 0
                }

                # Download progress callback
                def tg_progress(current, total):
                    if cancel_flags.get(task_id): raise asyncio.CancelledError
                    now = time.time()
                    elapsed = max(now - ACTIVE_TASKS[task_id]["start_time"], 0.001)
                    ACTIVE_TASKS[task_id]["current"] = current
                    ACTIVE_TASKS[task_id]["total"] = total
                    ACTIVE_TASKS[task_id]["speed"] = current / elapsed
                    ACTIVE_TASKS[task_id]["eta"] = (total - current) / ACTIVE_TASKS[task_id]["speed"] if ACTIVE_TASKS[task_id]["speed"] > 0 else 0

                try:
                    path = await download_with_retry(
                        client, media_msg, file_name=path,
                        chat_id=chat_id, msg_id=int(task_id),
                        progress=tg_progress
                    )
                except Exception as dl_err:
                    logger.error(f"Download failed for task {task_id}: {dl_err}")
                    with contextlib.suppress(Exception):
                        await client.send_message(
                            chat_id,
                            f"�?� <b>Download Failed:</b> <code>{safe_html(file_name)}</code>\n"
                            f"Error: <code>{safe_html(str(dl_err)[:200])}</code>\n\n"
                            f"💡 <i>Possible reasons:</i>\n"
                            f"• Telegram server timeout (large file)\n"
                            f"• Colab network issue\n"
                            f"• File too large for current session\n\n"
                            f"🔄 <b>Try again or use a smaller file.</b>",
                            parse_mode=enums.ParseMode.HTML
                        )
                    shutil.rmtree(dl_dir, ignore_errors=True)
                    return
                
                if not path or not os.path.exists(path):
                    logger.error(f"Downloaded file not found for task {task_id}")
                    with contextlib.suppress(Exception):
                        await client.send_message(
                            chat_id,
                            f"�?� <b>Download Error:</b> File <code>{safe_html(file_name)}</code> was not saved properly.\n"
                            f"🔄 <b>Please try again.</b>",
                            parse_mode=enums.ParseMode.HTML
                        )
                    shutil.rmtree(dl_dir, ignore_errors=True)
                    return
                
                logger.info(f"File downloaded successfully: {path} ({format_bytes(os.path.getsize(path))})")
                if action == "enhance4k":
                    status_msg = await client.send_message(chat_id, "🔮 <b>Starting 4K Enhancement...</b>", parse_mode=enums.ParseMode.HTML)
                    final_4k = await process_4k_enhancement(client, path, status_msg, task_id)
                    if final_4k and os.path.exists(final_4k):
                        file_size = os.path.getsize(final_4k)
                        if file_size > MAX_FILE_SIZE:
            await status.edit_text(
                f"⚠️ Final video is {file_size // (1024*1024)} MB (over 2GB limit).\n"
                f"Splitting into parts before upload...",
                parse_mode=enums.ParseMode.HTML
            )
            import glob
            out_dir = os.path.dirname(final_output)
            name, ext = os.path.splitext(os.path.basename(final_output))
            split_pattern = os.path.join(out_dir, f"{name}_pt%02d.mkv")
            cmd = ["mkvmerge", "-o", split_pattern, "--split", "size:1950M", final_output]
            proc = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
            await proc.communicate()
            parts = sorted(glob.glob(os.path.join(out_dir, f"{name}_pt*.mkv")))
            
            thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
            for p_idx, part_file in enumerate(parts):
                part_size = os.path.getsize(part_file)
                await status.edit_text(f"📤 <b>Uploading part {p_idx+1}/{len(parts)}...</b> ({part_size // (1024*1024)} MB)", parse_mode=enums.ParseMode.HTML)
                await client.send_document(
                    chat_id=message.chat.id,
                    document=part_file,
                    caption=f"🎬 <b>{basename} — 4K Enhanced (Part {p_idx+1})</b>\n10-bit HEVC x265 | Real-ESRGAN\n{WATERMARK}",
                    parse_mode=enums.ParseMode.HTML,
                    force_document=True,
                    thumb=thumb
                )
            await status.edit_text("✅ <b>Done!</b> Final 4K video split and uploaded successfully.")
        else:
            thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
            await client.send_document(
                chat_id=message.chat.id,
                document=final_output,
                caption=f"🎬 <b>{basename} — 4K Enhanced</b>\n10-bit HEVC x265 | Real-ESRGAN\n{WATERMARK}",
                parse_mode=enums.ParseMode.HTML,
                thumb=thumb,
                force_document=True
            )
            await status.edit_text("✅ <b>4K Enhancement Complete!</b> 🎉")
    except Exception as e:
        await status.edit_text(f"�?� Upload failed: {e}")
    
    # Cleanup
    shutil.rmtree(work_dir, ignore_errors=True)
    for f in [merged_path, hevc_path]:
        if os.path.exists(f): os.remove(f)

# --- START COMMAND ---
async def start_cmd(client, message):
    await message.reply(
        "⚡ <b>Colab Worker 3.0 is Online & Ready!</b>\n\n"
        "• Use <code>/leech &lt;magnet_link&gt;</code> to download torrents.\n"
        "• Use <code>/enhance</code> (reply to video) or <code>/enhance &lt;url&gt;</code> for <b>4K Upscaling</b>.\n"
        "• Send or reply to any video to open the <b>Encoding Panel</b>.\n"
        "• Use <code>/help</code> for commands list (සිංහල).",
        parse_mode=enums.ParseMode.HTML
    )

# --- START WEBSOCKET LOOP ---
async def heartbeat_loop(websocket):
    try:
        while True:
            await asyncio.sleep(10)
            await websocket.send("ping")
            await websocket.recv()
    except websockets.exceptions.ConnectionClosed:
        logger.warning("Master server disconnected!")

async def main():
    global app, user_app, upload_client, aria2_api
    
    aria2_api = aria2p.API(aria2p.Client(host="http://localhost", port=6800, secret=""))
    subprocess.Popen(["aria2c", "--enable-rpc=true", "--rpc-listen-all=false", "--rpc-listen-port=6800", "--daemon=true"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    
    key = str(random.randint(100000, 999999))
    print(f"YOUR CONNECTION KEY IS: {key}")
    
    try:
        async with websockets.connect(MASTER_WS_URL) as ws:
            await ws.send(key)
            response = await ws.recv()
            data = json.loads(response)
            
            if data.get("status") != "AUTHORIZED": return
            
            api_id, api_hash, bot_token = data.get("API_ID"), data.get("API_HASH"), data.get("BOT_TOKEN")
            app = Client("colab_worker", api_id=api_id, api_hash=api_hash, bot_token=bot_token, in_memory=True)
            upload_client = app
            
            # Register Handlers for Private AND Group chats
            app.add_handler(MessageHandler(start_cmd, filters.command("start")))
            app.add_handler(MessageHandler(help_cmd, filters.command("help")))
            app.add_handler(MessageHandler(testdb_cmd, filters.command("testdb")))
            app.add_handler(MessageHandler(panel_cmd, filters.command(["panel", "encode"])))
            app.add_handler(MessageHandler(handle_leech, filters.command("leech")))
            app.add_handler(MessageHandler(handle_enhance, filters.command("enhance")))
            app.add_handler(MessageHandler(handle_url, filters.command("url")))
            app.add_handler(MessageHandler(cancel_cmd, filters.regex(r"^/cancel")))
            app.add_handler(MessageHandler(handle_telegram_file, (filters.document | filters.video)))
            app.add_handler(MessageHandler(save_thumbnail, filters.photo))
            app.add_handler(MessageHandler(reply_handler, filters.reply))
            app.add_handler(CallbackQueryHandler(cb_handler))
            
            asyncio.create_task(heartbeat_loop(ws))
            asyncio.create_task(global_ui_loop())
            await app.start()
            logger.info("✅ Colab Worker 3.0 is ONLINE with Global Progress UI & Multi-Tasking!")
            await ws.wait_closed()
            
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
    finally:
        if app and app.is_connected: await app.stop()

if __name__ == "__main__":
    asyncio.run(main())











