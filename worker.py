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
            logger.warning(f"❌ Download attempt {attempt}/{max_retries} failed: {e}")
            if attempt == max_retries:
                break
            # Longer backoff to let DC5 cooldown: 30s, 60s, 120s, 180s, 180s...
            wait_time = min(30 * (2 ** (attempt - 1)), 180)
            logger.info(f"⏳ Waiting {wait_time}s before retry (fresh session + file_reference)...")
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
                lines.append(f"{prefix}{connector}📁 {html.escape(key) if is_html else key}")
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
        msg = await app.send_message(chat_id, "⏳ <b>Starting task...</b>", parse_mode=enums.ParseMode.HTML)
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

async def encode_video(input_path, output_path, resolution, total_duration, task_id, filename):
    has_nvenc = check_gpu()
    if resolution == 1080: scale, cq, crf = "scale=-2:'min(1080,ih)'", "30", "28"
    elif resolution == 720: scale, cq, crf = "scale=-2:'min(720,ih)'", "34", "32"
    else: scale, cq, crf = "scale=-2:'min(480,ih)'", "38", "36"

    if has_nvenc:
        vcodec = ["-c:v", "h264_nvenc", "-preset", "p6", "-tune", "hq", "-cq", cq, "-pix_fmt", "yuv420p"]
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
         btn("1080p", f"panel_1080_{task_id}", style=enums.ButtonStyle.PRIMARY)],
        [btn("🔮 4K (6B)", f"panel_enhance4k_{task_id}", style=enums.ButtonStyle.SUCCESS),
         btn("⚡ 4K (v3)", f"panel_enhance4kv3_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("🚀 4K (Video2X)", f"panel_enhance4kvideo2x_{task_id}", style=enums.ButtonStyle.SUCCESS),
         btn("🎭 4K (CUGAN)", f"panel_enhance4kcugan_{task_id}", style=enums.ButtonStyle.SUCCESS)],
         [btn("🎭 4K (NCNN-Vulkan)", f"panel_enhance4kvulkan_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("✂️ Remove Sub", f"panel_removesub_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("📝 Add Sub", f"panel_addsub_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("🔍 Extract Sub", f"panel_extract_sub_{task_id}", style=enums.ButtonStyle.DEFAULT)],
        [btn("🎵 Extract Audio", f"panel_extract_audio_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("✏️ Rename", f"panel_rename_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("🔄 Re-encode All", f"panel_reencode_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("🚀 Upload Now", f"panel_upload_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("❌ Cancel", f"panel_cancel_{task_id}", style=enums.ButtonStyle.DANGER)]
    ])

# Sinhala Help Menu
async def testdb_cmd(client, message):
    try:
        await client.send_message(PRIVATE_DB_CHANNEL, "✅ <b>Test Message:</b> DB Channel Connection is Working!", parse_mode=enums.ParseMode.HTML)
        await message.reply_text("✅ Message sent to DB Channel successfully!")
    except Exception as e:
        await message.reply_text(f"❌ Failed to send to DB Channel: {e}")

async def help_cmd(client, message):
    text = (
        "<b>📚 Super Encoder & Leech Bot Help Menu</b>\n\n"
        "<b>📥 ටොරන්ට් භාගත කිරීම:</b>\n"
        "<code>/leech &lt;magnet_link&gt;</code> - Torrent එකක් භාගත කර File list එක ලබා ගෙන අවශ්‍ය files තෝරාගැනීමට.\n\n"
        "<b>🎨 Encoding Panel Commands:</b>\n"
        "පහත Commands ඔයාට Panel එකේ බොත්තම් විදියට වගේම, කෙලින්ම <b>වීඩියෝ එකකට Reply කරලත්</b> පාවිච්චි කරන්න පුළුවන්.\n\n"
        "<b>🎬 Video Resolutions:</b>\n"
        "<code>/480</code> - 480p වලට Convert කිරීම.\n"
        "<code>/720</code> - 720p වලට Convert කිරීම.\n"
        "<code>/1080</code> - 1080p වලට Convert කිරීම.\n"
        "<code>/reencode</code> - 480p, 720p, 1080p සහ Original එකත් එක්ක File 4ක්ම ලබා දීම.\n\n"
        "<b>✂️ Subtitles:</b>\n"
        "<code>/removesub</code> - Soft subtitles අයින් කිරීම.\n"
        "<code>/addsub</code> - Subtitle එකතු කිරීම (වීඩියෝ එකකට subtitle file එකක් reply කරන්න).\n"
        "<code>/extract_sub</code> - Subtitle එක වෙනම ගලවාගැනීම.\n\n"
        "<b>🎵 Audio:</b>\n"
        "<code>/addaudio</code> - අලුත් Audio Track එකක් දැමීම.\n"
        "<code>/extract_audio</code> - Audio එක වෙනම ගලවාගැනීම.\n"
        "<code>/remaudio</code> - Audio Track එක අයින් කිරීම.\n\n"
        "<b>🖼 Thumbnail:</b>\n"
        "<code>/extract_thumb</code> - වීඩියෝ එකේ Thumbnail එක ගලවාගැනීම.\n\n"
        "<b>🛑 Tasks නැවැත්වීම:</b>\n"
        "<code>/cancel_&lt;task_id&gt;</code> - ඕනෑම ක්‍රියාවලියක් නතර කිරීමට."
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
    if action in ["480", "720", "1080", "reencode"]:
        res_list = [int(action)] if action != "reencode" else [480, 720, 1080]
        if action == "reencode": files_to_upload.append(filepath) # Keep original
        
        for res in res_list:
            if cancel_flags.get(task_id): break
            out_file = generate_res_name(filename, res) + ".mkv"
            out_path = os.path.join(os.path.dirname(filepath), out_file)
            success = await encode_video(filepath, out_path, res, duration, task_id, out_file)
            if success:
                out_path = await apply_mkv_watermark(out_path)
                files_to_upload.append(out_path)
    elif action == "removesub":
        out_path = os.path.join(os.path.dirname(filepath), "nosub_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-map", "0:v", "-map", "0:a?", "-c", "copy", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "✂️ Removing Subs", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    elif action == "addsub" and sub_path:
        out_path = os.path.join(os.path.dirname(filepath), "sub_" + filename)
        cmd = ["ffmpeg", "-y", "-i", filepath, "-i", sub_path, "-c", "copy", "-c:s", "srt", "-metadata:s:s:0", f"title={WATERMARK}", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, task_id, "📝 Adding Subtitle", filename)
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
            await client.send_message(chat_id, f"⚠️ <b>No subtitles found</b> or file is not MKV: <code>{filename}</code>", parse_mode=enums.ParseMode.HTML)
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
                await client.send_message(chat_id, f"⚠️ <b>Failed to extract subtitles</b> from: <code>{filename}</code>", parse_mode=enums.ParseMode.HTML)
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
    for f_path in files_to_upload:
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
                                f"⚠️ <b>Database Warning:</b> Bot cannot post to <code>{TARGET_CHANNEL}</code>.\n"
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
                    f"❌ <b>Upload Failed:</b> <code>{safe_html(f_name)}</code>\n"
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
                            f"❌ <b>Download Failed:</b> <code>{safe_html(file_name)}</code>\n"
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
                            f"❌ <b>Download Error:</b> File <code>{safe_html(file_name)}</code> was not saved properly.\n"
                            f"🔄 <b>Please try again.</b>",
                            parse_mode=enums.ParseMode.HTML
                        )
                    shutil.rmtree(dl_dir, ignore_errors=True)
                    return
                
                logger.info(f"File downloaded successfully: {path} ({format_bytes(os.path.getsize(path))})")
                if action in ("enhance4k", "enhance4kv3", "enhance4kvideo2x", "enhance4kcugan", "enhance4kvulkan"):
                    status_msg = await client.send_message(chat_id, "🔮 <b>Starting 4K Enhancement...</b>", parse_mode=enums.ParseMode.HTML)
                    final_4k = await process_4k_enhancement(client, path, status_msg, task_id, model=action.replace("enhance4k", "") if action != "enhance4k" else "6B")
                    if final_4k and os.path.exists(final_4k):
                        file_size = os.path.getsize(final_4k)
                        if file_size > MAX_FILE_SIZE:
                            await client.send_document(chat_id=PRIVATE_DB_CHANNEL, document=final_4k,
                                caption=f"🎬 4K FINAL | {os.path.basename(final_4k)}\n#FINAL", force_document=True)
                            await status_msg.edit_text("✅ <b>Done!</b> Final 4K video saved to DB Channel (too large for chat).")
                        else:
                            thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
                            await client.send_document(
                                chat_id=chat_id, document=final_4k,
                                caption=f"🎬 <b>{os.path.basename(final_4k)} — 4K Enhanced</b>\n10-bit HEVC x265 | Real-ESRGAN\n{WATERMARK}",
                                parse_mode=enums.ParseMode.HTML, thumb=thumb, force_document=True
                            )
                            await status_msg.edit_text("✅ <b>4K Enhancement Complete!</b> 🎉", parse_mode=enums.ParseMode.HTML)
                        if os.path.exists(final_4k): os.remove(final_4k)
                else:
                    await process_media_file(client, chat_id, path, action, custom_renames, 1, task_id, sub_path, audio_path)
                shutil.rmtree(dl_dir, ignore_errors=True)

            elif not is_telegram_file and task_id in current_tasks:
                t_data = current_tasks.pop(task_id)
                if not custom_renames: custom_renames = t_data.get("custom_renames", {})
                dl_dir = f"/content/dl_{task_id}"
                os.makedirs(dl_dir, exist_ok=True)
                global aria2_api
                
                if "url" in t_data:
                    dl = aria2_api.add_uris([t_data["url"]], options={"dir": dl_dir})
                    dl_type = "URL"
                else:
                    dl = aria2_api.add_torrent(t_data["torrent"], options={"dir": dl_dir, "select-file": ",".join(map(str, t_data["selected"]))})
                    dl_type = "Torrent"
                
                ACTIVE_TASKS[task_id] = {
                    "task_id": task_id,
                    "chat_id": chat_id,
                    "user_mention": t_data.get("user_mention", "User"),
                    "filename": t_data["t_name"],
                    "status": f"📥 Downloading {dl_type}",
                    "current": 0,
                    "total": 1,
                    "is_time": False,
                    "start_time": time.time(),
                    "speed": 0,
                    "eta": 0
                }

                while dl.status not in ["complete", "error", "removed"]:
                    await asyncio.sleep(2)
                    dl.update()
                    if cancel_flags.get(task_id):
                        aria2_api.remove([dl], force=True, files=False)
                        break
                    if dl.total_length > 0:
                        ACTIVE_TASKS[task_id].update({
                            "current": dl.completed_length,
                            "total": dl.total_length,
                            "speed": dl.download_speed,
                            "eta": dl.eta.total_seconds() if dl.eta else 0
                        })
                        if dl.completed_length >= dl.total_length:
                            aria2_api.remove([dl], force=True, files=False)
                            break
                        
                if not cancel_flags.get(task_id):
                    for f in dl.files:
                        if (getattr(f, "selected", False) or "url" in t_data) and os.path.exists(str(f.path)):
                            if action in ("enhance4k", "enhance4kv3", "enhance4kvideo2x", "enhance4kcugan", "enhance4kvulkan"):
                                # 4K Enhancement Pipeline
                                status_msg = await client.send_message(chat_id, "🔮 <b>Starting 4K Enhancement...</b>", parse_mode=enums.ParseMode.HTML)
                                final_4k = await process_4k_enhancement(client, str(f.path), status_msg, task_id, model=action.replace("enhance4k", "") if action != "enhance4k" else "6B")
                                if final_4k and os.path.exists(final_4k):
                                    file_size = os.path.getsize(final_4k)
                                    if file_size > MAX_FILE_SIZE:
                                        await client.send_document(chat_id=PRIVATE_DB_CHANNEL, document=final_4k,
                                            caption=f"🎬 4K FINAL | {os.path.basename(final_4k)}\n#FINAL", force_document=True)
                                        await status_msg.edit_text("✅ <b>Done!</b> Final 4K video saved to DB Channel (too large for chat).")
                                    else:
                                        thumb = CUSTOM_THUMB_PATH if os.path.exists(CUSTOM_THUMB_PATH) else None
                                        await client.send_document(
                                            chat_id=chat_id, document=final_4k,
                                            caption=f"🎬 <b>{os.path.basename(final_4k)} — 4K Enhanced</b>\n10-bit HEVC x265 | Real-ESRGAN\n{WATERMARK}",
                                            parse_mode=enums.ParseMode.HTML, thumb=thumb, force_document=True
                                        )
                                        await status_msg.edit_text("✅ <b>4K Enhancement Complete!</b> 🎉", parse_mode=enums.ParseMode.HTML)
                                    if os.path.exists(final_4k): os.remove(final_4k)
                            else:
                                await process_media_file(client, chat_id, str(f.path), action, custom_renames, f.index, task_id)
                shutil.rmtree(dl_dir, ignore_errors=True)

        except asyncio.CancelledError:
            logger.info(f"Task {task_id} was cancelled.")
            with contextlib.suppress(Exception):
                await client.send_message(chat_id, f"🛑 <b>Task cancelled.</b>", parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            logger.error(f"Task {task_id} error: {e}")
            with contextlib.suppress(Exception):
                await client.send_message(
                    chat_id,
                    f"❌ <b>Task Error:</b>\n<code>{safe_html(str(e)[:300])}</code>\n\n🔄 <b>Please try again.</b>",
                    parse_mode=enums.ParseMode.HTML
                )
        finally:
            ACTIVE_TASKS.pop(task_id, None)
            cancel_flags.pop(task_id, None)

# --- TELEGRAM FILE HANDLER (PANEL TRIGGER FOR GROUPS & PRIVATE) ---
async def handle_telegram_file(client, message):
    try:
        media = message.document or message.video
        if not media: return
        file_name = getattr(media, "file_name", None)
        if not file_name:
            if message.video: file_name = f"video_{message.id}.mp4"
            else: file_name = f"file_{message.id}"
        task_id = str(message.id)
        
        await message.reply_text(
            f"<b>🎬 File Detected:</b>\n<code>{safe_html(file_name)}</code>\n\n"
            "👉 <i>Choose an action from the Encoding Panel below:</i>",
            reply_markup=get_panel_markup(task_id),
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
    except Exception as e:
        logger.error(f"Error in handle_telegram_file: {e}")

# --- PANEL & ENCODE COMMAND HANDLER (GREAT FOR GROUPS) ---
async def panel_cmd(client, message):
    try:
        media_msg = message.reply_to_message
        if not media_msg or not (media_msg.document or media_msg.video):
            if message.document or message.video:
                media_msg = message
            else:
                return await message.reply("👉 <i>Please reply to a video or file with <code>/panel</code> to open the Encoding Panel.</i>", parse_mode=enums.ParseMode.HTML)
        
        media = media_msg.document or media_msg.video
        file_name = getattr(media, "file_name", None)
        if not file_name:
            if media_msg.video: file_name = f"video_{media_msg.id}.mp4"
            else: file_name = f"file_{media_msg.id}"
            
        task_id = str(media_msg.id)
        await message.reply_text(
            f"<b>🎬 File Detected:</b>\n<code>{safe_html(file_name)}</code>\n\n"
            "👉 <i>Choose an action from the Encoding Panel below:</i>",
            reply_markup=get_panel_markup(task_id),
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
    except Exception as e:
        logger.error(f"Error in panel_cmd: {e}")

# --- LEECH COMMAND ---
async def handle_leech(client, message):
    if len(message.command) < 2:
        return await message.reply("Please provide a magnet link! Example: <code>/leech magnet:?...</code>", parse_mode=enums.ParseMode.HTML)
    magnet = message.command[1]
    
    status_msg = await message.reply("🔍 <i>Fetching torrent metadata...</i>", parse_mode=enums.ParseMode.HTML)
    temp_dir = f"/content/meta_{message.id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        cmd = ["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true", "--console-log-level=error", "--dir", temp_dir, magnet]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        
        t_file = next((os.path.join(temp_dir, n) for n in os.listdir(temp_dir) if n.endswith(".torrent")), None)
        if not t_file: return await status_msg.edit_text("❌ Failed to fetch torrent metadata.")
        
        global aria2_api
        
        # Make a copy of the .torrent file for metadata extraction so it doesn't get deleted
        t_file_meta = t_file + ".meta.torrent"
        import shutil
        shutil.copy(t_file, t_file_meta)
        
        meta_dl = aria2_api.add_torrent(t_file_meta, options={"pause": "true"})
        files, t_name = meta_dl.files, meta_dl.name
        aria2_api.remove([meta_dl], force=True, files=False)
        
        tree_html = build_file_tree(files, True)
        tree_txt = build_file_tree(files, False)
        
        caption = (f"✅ <b>Torrent Identified!</b>\n\n"
                   f"👉 <b>Reply to this message</b> with file numbers to download.\n"
                   f"Example: <code>1,3,5-7</code> (or <code>all</code> for everything)")
        
        if len(tree_html) > 3500:
            txt_path = os.path.join(temp_dir, "file_list.txt")
            with open(txt_path, "w") as f: f.write(tree_txt)
            prompt = await message.reply_document(document=txt_path, caption=caption, parse_mode=enums.ParseMode.HTML)
        else:
            prompt = await message.reply_text(f"<b>Files:</b>\n{tree_html}\n\n{caption}", parse_mode=enums.ParseMode.HTML)
            
        task_id = str(prompt.id)
        current_tasks[task_id] = {
            "torrent": t_file,
            "files": files,
            "t_name": t_name,
            "temp": temp_dir,
            "user_mention": message.from_user.mention if message.from_user else "User"
        }
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")

# --- URL COMMAND ---
async def handle_url(client, message):
    if len(message.command) < 2:
        return await message.reply("Please provide a direct URL! Example: <code>/url https://domain.com/video.mkv</code>", parse_mode=enums.ParseMode.HTML)
    url = message.command[1]
    
    file_name = url.split("/")[-1].split("?")[0]
    if not file_name or len(file_name) < 3: file_name = "downloaded_video.mkv"
    
    task_id = str(message.id)
    current_tasks[task_id] = {
        "url": url,
        "t_name": file_name,
        "user_mention": message.from_user.mention if message.from_user else "User"
    }
    
    await message.reply_text(
        f"<b>🔗 URL Detected:</b>\n<code>{safe_html(url)}</code>\n\n"
        "👉 <i>Choose an action from the Encoding Panel below:</i>",
        reply_markup=get_panel_markup(task_id),
        parse_mode=enums.ParseMode.HTML,
        quote=True
    )

# --- THUMBNAIL HANDLER ---
async def save_thumbnail(client, message):
    if message.photo:
        status = await message.reply("🖼️ <i>Downloading thumbnail...</i>", parse_mode=enums.ParseMode.HTML)
        try:
            await message.download(file_name=CUSTOM_THUMB_PATH)
            await status.edit_text("✅ <b>Custom thumbnail saved successfully!</b>\nIt will be used for all future uploads.", parse_mode=enums.ParseMode.HTML)
        except Exception as e:
            await status.edit_text(f"❌ <b>Error saving thumbnail:</b>\n{e}", parse_mode=enums.ParseMode.HTML)

# --- REPLY COMMANDS & FILE SELECTION ---
async def reply_handler(client, message):
    if not message.reply_to_message: return
    r_id = str(message.reply_to_message.id)
    raw_text = (message.text or message.caption or "").strip()
    text = raw_text.lower()
    # Check pending rename reply
    if r_id in pending_renames:
        r_data = pending_renames.pop(r_id)
        new_name = raw_text
        task_id = r_data["task_id"]
        
        if r_data["is_tg"]:
            media_msg = r_data["media_msg"]
            asyncio.create_task(execute_task_worker(client, message.chat.id, task_id, "upload", media_msg=media_msg, is_telegram_file=True, custom_renames={1: new_name}))
        else:
            if task_id in current_tasks:
                if "custom_renames" not in current_tasks[task_id]:
                    current_tasks[task_id]["custom_renames"] = {}
                sel = current_tasks[task_id]["selected"]
                if len(sel) == 1:
                    current_tasks[task_id]["custom_renames"][sel[0]] = new_name
                await message.reply("✅ <b>File renamed!</b> Choose action from panel.", parse_mode=enums.ParseMode.HTML)
        return

    # Check pending subtitle reply
    if r_id in pending_sub_replies and (message.document or message.video):
        media_msg = pending_sub_replies.pop(r_id)
        sub_dir = f"/content/sub_{message.id}"
        os.makedirs(sub_dir, exist_ok=True)
        sub_path = await client.download_media(message, file_name=sub_dir + "/")
        task_id = str(message.id)
        asyncio.create_task(execute_task_worker(client, message.chat.id, task_id, "addsub", media_msg=media_msg, is_telegram_file=True, sub_path=sub_path))
        return

    # Check pending audio reply
    if r_id in pending_audio_replies and (message.audio or message.document):
        media_msg = pending_audio_replies.pop(r_id)
        aud_dir = f"/content/aud_{message.id}"
        os.makedirs(aud_dir, exist_ok=True)
        aud_path = await client.download_media(message, file_name=aud_dir + "/")
        task_id = str(message.id)
        asyncio.create_task(execute_task_worker(client, message.chat.id, task_id, "addaudio", media_msg=media_msg, is_telegram_file=True, audio_path=aud_path))
        return

    # 1. Torrent File Selection
    if r_id in current_tasks:
        task_data = current_tasks.pop(r_id)
        selected = parse_selection(text, len(task_data["files"]))
        if not selected: return await message.reply("❌ Invalid file selection. Please enter numbers like <code>1,2,3</code> or <code>all</code>.", parse_mode=enums.ParseMode.HTML)
        
        task_data["selected"] = selected
        new_task_id = str(message.id)
        current_tasks[new_task_id] = task_data
        
        await message.reply_text(
            f"✅ <b>Files Selected!</b> Choose an action from the Encoding Panel:",
            reply_markup=get_panel_markup(new_task_id),
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
        return
        
    # 2. Direct Reply Commands on Media (/480, /removesub, /extract_sub, etc.)
    if message.reply_to_message.document or message.reply_to_message.video:
        if text.startswith("/"):
            action = text.replace("/", "").split()[0].replace(f"@{client.me.username}" if client.me else "", "")
            
            if action == "addsub":
                prompt = await message.reply("📝 <b>Please reply to THIS message with your subtitle file (.srt, .ass, etc.)</b>", parse_mode=enums.ParseMode.HTML)
                pending_sub_replies[str(prompt.id)] = message.reply_to_message
                return
            elif action == "addaudio":
                prompt = await message.reply("🎵 <b>Please reply to THIS message with your audio file (.aac, .m4a, .mp3, etc.)</b>", parse_mode=enums.ParseMode.HTML)
                pending_audio_replies[str(prompt.id)] = message.reply_to_message
                return
            elif action in ["480", "720", "1080", "reencode", "removesub", "extract_sub", "extract_audio", "remaudio", "extract_thumb", "enhance4k"]:
                task_id = str(message.id)
                asyncio.create_task(execute_task_worker(client, message.chat.id, task_id, action, media_msg=message.reply_to_message, is_telegram_file=True))

# --- PANEL CALLBACKS ---
async def cb_handler(client, cb):
    data = cb.data
    if data.startswith("panel_"):
        action = data[len("panel_"):].rsplit("_", 1)[0]
        task_id = data.rsplit("_", 1)[1]
        
        if action == "cancel":
            cancel_flags[task_id] = True
            return await cb.message.edit_text("🚫 <b>Task cancelled.</b>", parse_mode=enums.ParseMode.HTML)
        
        # ALWAYS fetch the full original message to ensure media data is available
        # callback query's reply_to_message often doesn't contain document/video data
        media_target = None
        try:
            media_target = await client.get_messages(cb.message.chat.id, int(task_id))
            if media_target and not (media_target.document or media_target.video):
                logger.warning(f"get_messages({task_id}): message found but no media, trying reply_to_message")
                media_target = None
        except Exception as e:
            logger.warning(f"get_messages({task_id}) failed: {e}")
        
        # Fallback to reply_to_message
        if not media_target:
            media_target = cb.message.reply_to_message
        
        if action == "addsub":
            await cb.message.edit_reply_markup(None)
            prompt = await cb.message.reply("📝 <b>Please reply to THIS message with your subtitle file (.srt, .ass, etc.)</b>", parse_mode=enums.ParseMode.HTML)
            pending_sub_replies[str(prompt.id)] = media_target
            return

        if action == "rename":
            await cb.message.edit_reply_markup(None)
            prompt = await cb.message.reply("✏️ <b>Please reply to THIS message with the NEW NAME (with extension, e.g. video.mkv):</b>", parse_mode=enums.ParseMode.HTML)
            is_tg = (task_id not in current_tasks)
            pending_renames[str(prompt.id)] = {"task_id": task_id, "is_tg": is_tg, "media_msg": media_target}
            return

        await cb.message.edit_reply_markup(None)
        is_tg = (task_id not in current_tasks)
        
        # Final validation: make sure we have a valid media message
        if is_tg and (not media_target or not (media_target.document or media_target.video)):
            logger.error(f"No valid media found for task {task_id}")
            await cb.message.reply(
                "❌ <b>Error:</b> Could not find the original file message.\n"
                "🔄 <b>Please re-send the video and try again.</b>",
                parse_mode=enums.ParseMode.HTML
            )
            return
        
        logger.info(f"Starting task {task_id}: action={action}, is_tg={is_tg}, "
                     f"has_doc={bool(media_target and media_target.document)}, "
                     f"has_vid={bool(media_target and media_target.video)}")
        asyncio.create_task(execute_task_worker(client, cb.message.chat.id, task_id, action, media_msg=media_target, is_telegram_file=is_tg))

# --- CANCEL COMMAND ---
async def cancel_cmd(client, message):
    cmd_text = message.text.strip()
    task_id = None
    if "_" in cmd_text:
        task_id = cmd_text.split("_", 1)[1].split()[0]
    elif len(message.command) > 1:
        task_id = message.command[1]
        
    if task_id and (task_id in ACTIVE_TASKS or task_id in cancel_flags):
        cancel_flags[task_id] = True
        await message.reply(f"🛑 <b>Task <code>{task_id}</code> is stopping...</b>", parse_mode=enums.ParseMode.HTML)
    else:
        await message.reply("❌ Task not found or already finished.")

# ============================================================
# --- 4K REAL-ESRGAN ENHANCEMENT SYSTEM (Auto-Resume) ---
# ============================================================

async def _run_shell(cmd):
    """Run a shell command asynchronously"""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    stdout, stderr = await proc.communicate()
    return proc.returncode, stdout.decode(errors='ignore'), stderr.decode(errors='ignore')

async def _split_video_to_chunks(input_video, output_dir, segment_seconds=60):
    """FFmpeg: Split video into N-second segments"""
    os.makedirs(output_dir, exist_ok=True)
    basename = os.path.splitext(os.path.basename(input_video))[0]
    pattern = os.path.join(output_dir, f"{basename}_chunk_%03d.mp4")
    cmd = [
        "ffmpeg", "-y", "-i", input_video,
        "-c", "copy", "-f", "segment",
        "-segment_time", str(segment_seconds),
        "-reset_timestamps", "1", pattern
    ]
    await _run_shell(cmd)
    return sorted(glob.glob(os.path.join(output_dir, f"{basename}_chunk_*.mp4")))

async def _upscale_chunk_realesrgan(input_path, output_path, status_msg=None, prefix_text="", model="6B", progress_callback=None):
    """Run Real-ESRGAN inference on a single chunk with real-time progress"""
    import time
    import re
    if model == "video2x":
        bash_cmd = f"export PATH=/content/realesrgan_vulkan:$PATH && video2x -i '{input_path}' -o '{output_path}' -p realesrgan -s 4 --realesrgan-model realesr-animevideov3 1>&2"
        cmd = ["bash", "-c", bash_cmd]
    elif model == "cugan":
        tmp_in = output_path + "_tmp_in"
        tmp_out = output_path + "_tmp_out"
        bash_cmd = (
            f"mkdir -p '{tmp_in}' '{tmp_out}' && "
            f"ffmpeg -hide_banner -loglevel error -i '{input_path}' '{tmp_in}/%08d.jpg' && "
            f"cd /content/realcugan && chmod +x realcugan-ncnn-vulkan && ./realcugan-ncnn-vulkan -i '{tmp_in}' -o '{tmp_out}' -s 2 -n 2 -f jpg && "
            f"FPS=$(ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of default=noprint_wrappers=1:nokey=1 '{input_path}') && "
            f"ffmpeg -hide_banner -loglevel error -framerate $FPS -i '{tmp_out}/%08d.jpg' -i '{input_path}' -map 0:v -map 1:a? -c:v libx264 -crf 20 -c:a copy '{output_path}' && "
            f"rm -rf '{tmp_in}' '{tmp_out}'"
        )
        cmd = ["bash", "-c", bash_cmd]
    elif model == "vulkan":
        tmp_in = output_path + "_tmp_in"
        tmp_out = output_path + "_tmp_out"
        bash_cmd = (
            f"mkdir -p '{tmp_in}' '{tmp_out}' && "
            f"ffmpeg -hide_banner -loglevel error -i '{input_path}' '{tmp_in}/%08d.jpg' && "
            f"cd /content/realesrgan_vulkan && chmod +x realesrgan-ncnn-vulkan && ./realesrgan-ncnn-vulkan -i '{tmp_in}' -o '{tmp_out}' -n realesrgan-x4plus-anime -s 4 -f jpg && "
            f"FPS=$(ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of default=noprint_wrappers=1:nokey=1 '{input_path}') && "
            f"ffmpeg -hide_banner -loglevel error -framerate $FPS -i '{tmp_out}/%08d.jpg' -i '{input_path}' -map 0:v -map 1:a? -c:v libx264 -crf 20 -c:a copy '{output_path}' && "
            f"rm -rf '{tmp_in}' '{tmp_out}'"
        )
        cmd = ["bash", "-c", bash_cmd]
    else:
        cmd = [
            "python", "/content/Real-ESRGAN/inference_realesrgan_video.py",
            "-n", "realesr-animevideov3" if model == "v3" else "RealESRGAN_x4plus_anime_6B",
            "-i", input_path,
            "-o", output_path,
            "--outscale", "2" if model == "v3" else "4"
        ]
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    last_update = time.time()
    last_frame = 0
    err_acc = ""
    while True:
        line = await proc.stderr.read(1024)
        if not line:
            break
        text = line.decode('utf-8', errors='ignore')
        err_acc += text
        
        if (status_msg or progress_callback) and ("%" in text or "it" in text):
            parts = text.split("\r")
            clean = parts[-1].strip() if parts else text.strip()
            
            # Extract frame number like "23/1440" or percentage like "48.33%"
            matches = re.findall(r'(\d+)/\d+', text)
            match = re.search(r'(\d+)/\d+', clean) if not matches else True
            pct_match = re.search(r'(\d+\.\d+)%', clean)
            if matches:
                clean = text.strip()
            now = time.time()
            if match:
                current_frame = int(matches[-1]) if "matches" in locals() and matches else int(match.group(1))
                if current_frame - last_frame >= 1 or now - last_update > 8:
                    last_frame = current_frame
                    if clean:
                        if progress_callback:
                            progress_callback(clean)
                        elif status_msg:
                            try:
                                await status_msg.edit_text(f"{prefix_text}\n\n⏳ <b>AI Processing:</b>\n<code>{clean}</code>", parse_mode=enums.ParseMode.HTML)
                                last_update = now
                            except: pass
            elif pct_match:
                pct = float(pct_match.group(1))
                elapsed = now - (last_frame if last_frame > 100000 else time.time() - 1)  # Hack: use last_frame as start_time if it's large
                if last_frame < 100000: last_frame = now  # Initialize start_time in last_frame
                
                if pct > 0.1 and now - last_update > 3:
                    total_time_est = (now - last_frame) / (pct / 100.0)
                    rem_time = max(0, total_time_est - (now - last_frame))
                    rem_mins = int(rem_time // 60)
                    rem_secs = int(rem_time % 60)
                    
                    frames_est = int((pct / 100.0) * 192)
                    
                    formatted_clean = f"[Kframe={frames_est}/192 ({pct:.2f}%); remaining={rem_mins:02d}:{rem_secs:02d}]"
                    
                    if progress_callback:
                        progress_callback(formatted_clean)
                    elif status_msg:
                        try:
                            await status_msg.edit_text(f"{prefix_text}\n\n⏳ <b>AI Processing:</b>\n<code>{formatted_clean}</code>", parse_mode=enums.ParseMode.HTML)
                            last_update = now
                        except: pass
            else:
                if now - last_update > 8:
                    if clean:
                        if progress_callback:
                            progress_callback(clean)
                        elif status_msg:
                            try:
                                await status_msg.edit_text(f"{prefix_text}\n\n⏳ <b>AI Processing:</b>\n<code>{clean}</code>", parse_mode=enums.ParseMode.HTML)
                                last_update = now
                            except: pass

    await proc.wait()
    
    if model not in ["anime4k", "cugan", "video2x", "vulkan"]:
        import shutil
        basename = os.path.splitext(os.path.basename(input_path))[0]
        actual_file = os.path.join(output_path, f"{basename}_out.mp4")
        if os.path.exists(actual_file):
            tmp_dir = output_path + "_dir"
            os.rename(output_path, tmp_dir)
            shutil.move(os.path.join(tmp_dir, f"{basename}_out.mp4"), output_path)
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return os.path.exists(output_path), err_acc

async def _encode_hevc_10bit(input_path, output_path):
    """FFmpeg: Re-encode to 10-bit HEVC x265"""
    cmd = [
        "ffmpeg", "-y", "-i", input_path,
        "-c:v", "libx265", "-preset", "medium",
        "-x265-params", "profile=main10",
        "-pix_fmt", "yuv420p10le",
        "-crf", "18",
        "-c:a", "copy",
        output_path
    ]
    code, _, _ = await _run_shell(cmd)
    return code == 0 and os.path.exists(output_path)

async def _merge_chunks(chunk_files, output_path):
    """FFmpeg: Merge multiple video chunks back into one file"""
    list_file = "/content/_merge_list.txt"
    with open(list_file, "w") as f:
        for c in chunk_files:
            f.write(f"file '{os.path.abspath(c)}'\n")
    cmd = [
        "ffmpeg", "-y", "-f", "concat", "-safe", "0",
        "-i", list_file, "-c", "copy", output_path
    ]
    code, _, _ = await _run_shell(cmd)
    if os.path.exists(list_file): os.remove(list_file)
    return code == 0 and os.path.exists(output_path)

async def _find_completed_chunks(client, job_tag):
    """Search Private DB Channel for already-completed chunks (Resume)"""
    completed = {}
    try:
        tmp = await client.send_message(PRIVATE_DB_CHANNEL, "ping")
        await tmp.delete()
        msgs = await client.get_messages(PRIVATE_DB_CHANNEL, range(max(1, tmp.id - 200), tmp.id))
        for msg in msgs:
            if not msg or msg.empty: continue
            if msg.document or msg.video:
                caption = msg.caption or ""
                if job_tag in caption:
                    for line in caption.split("\n"):
                        line = line.strip()
                        if line.startswith("#") and "_Chunk_" in line:
                            completed[line] = msg
    except Exception as e:
        logger.warning(f"Resume search failed: {e}")
    return completed

async def process_4k_enhancement(client, input_video_path, status_msg, task_id, model="6B"):
    """Main 4K pipeline: split → resume check → upscale → merge → encode"""
    basename = os.path.splitext(os.path.basename(input_video_path))[0]
    job_tag = f"{basename}_{model}".replace(" ", "_").replace(".", "_").replace("-", "_")
    
    work_dir = f"/content/4k_work_{job_tag}"
    raw_dir = os.path.join(work_dir, "raw_chunks")
    upscaled_dir = os.path.join(work_dir, "4k_chunks")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(upscaled_dir, exist_ok=True)
    
    # Step 1: Split
    await status_msg.edit_text("✂️ <b>Step 1/4:</b> Splitting video into 1-min chunks...", parse_mode=enums.ParseMode.HTML)
    raw_chunks = await _split_video_to_chunks(input_video_path, raw_dir, segment_seconds=8)
    total = len(raw_chunks)
    if total == 0:
        await status_msg.edit_text("❌ FFmpeg split failed - no chunks created.")
        return None
    
    await status_msg.edit_text(f"✂️ Split into <b>{total}</b> chunks.", parse_mode=enums.ParseMode.HTML)
    
    # Step 2: Resume check
    await status_msg.edit_text("🔍 <b>Step 2/4:</b> Checking DB Channel for previous progress...", parse_mode=enums.ParseMode.HTML)
    existing = await _find_completed_chunks(client, job_tag)
    skipped = 0
    
    # Step 3: Upscale chunks (Parallel for lightweight models)
    max_concurrent = 3 if model in ("v3", "anime4k") else (2 if model == "cugan" else 1)
    semaphore = asyncio.Semaphore(max_concurrent)
    
    completed_chunks = 0
    failed_chunks = 0
    final_chunks = [None] * total
    chunk_progress = {}
    
    async def process_single_chunk(i, raw_chunk):
        nonlocal completed_chunks, failed_chunks, skipped
        chunk_tag = f"#{job_tag}_Chunk_{i:03d}"
        out_name = os.path.basename(raw_chunk).replace(".mp4", "_4k.mp4")
        out_path = os.path.join(upscaled_dir, out_name)
        
        async with semaphore:
            if chunk_tag in existing:
                try:
                    await client.download_media(existing[chunk_tag], file_name=out_path)
                    skipped += 1
                    completed_chunks += 1
                    final_chunks[i] = out_path
                    return True
                except Exception as e:
                    logger.error(f"Failed to download {chunk_tag} from DB: {e}")
            
            # Upscale
            # We don't pass status_msg to prevent floodwaits during parallel processing
            def prog_cb(text):
                chunk_progress[i] = text
            success, err_msg = await _upscale_chunk_realesrgan(raw_chunk, out_path, None, "", model=model, progress_callback=prog_cb)
            if not success:
                logger.error(f"Chunk {i} failed: {err_msg}")
                failed_chunks += 1
                return False
                
        # Upload to DB channel OUTSIDE the semaphore so GPU isn't idle during upload!
        try:
            await client.send_document(
                chat_id=PRIVATE_DB_CHANNEL,
                document=out_path,
                caption=f"🎬 4K Chunk |\n{basename}\n{chunk_tag}\n{WATERMARK}"
            )
            logger.info(f"Uploaded {chunk_tag} to DB Channel.")
        except Exception as e:
            logger.error(f"Failed to upload {chunk_tag} to DB: {e}")
            
        completed_chunks += 1
        final_chunks[i] = out_path
        return True

    # Start a background task to update status message periodically
    async def update_status():
        while completed_chunks + failed_chunks < total:
            try:
                prog_text = "\n".join([f"▶️ Chunk {k+1}: <code>{v}</code>" for k, v in chunk_progress.items() if v and not (final_chunks[k] or k < completed_chunks)])
                if prog_text: prog_text = "\n\n<b>Live Progress:</b>\n" + prog_text
                
                await status_msg.edit_text(
                    f"⚙️ <b>Parallel Upscaling ({max_concurrent}x)...</b>\n"
                    f"✅ Completed: {completed_chunks}/{total}\n"
                    f"⏩ Skipped: {skipped}\n"
                    f"❌ Failed: {failed_chunks}{prog_text}", 
                    parse_mode=enums.ParseMode.HTML
                )
            except Exception:
                pass
            await asyncio.sleep(5)
            
    status_task = asyncio.create_task(update_status())
    
    # Run all chunks concurrently with semaphore limits
    tasks = [process_single_chunk(i, chunk) for i, chunk in enumerate(raw_chunks)]
    results = await asyncio.gather(*tasks)
    
    status_task.cancel()
    
    if not all(results):
        await status_msg.edit_text("❌ Upscaling failed for some chunks. Please check logs.")
        return None
        
        if task_id in ACTIVE_TASKS:
            ACTIVE_TASKS[task_id].update({
                "status": f"🚀 4K: Chunk {i+1}/{total}"
            })
            
        msg_text = f"🚀 <b>Step 3/4: Upscaling Chunk {i+1}/{total}</b> to 4K...\n⏩ Skipped: {skipped} | ⏳ Remaining after this: {total - i - 1}\n\n<i>This may take 15-40 min per chunk depending on GPU.</i>"
        await status_msg.edit_text(msg_text, parse_mode=enums.ParseMode.HTML)
        
        success, err_msg = await _upscale_chunk_realesrgan(raw_chunk, out_path, status_msg, msg_text, model=model)
        if not success:
            err_snippet = safe_html(str(err_msg)[-800:]) if err_msg else "Unknown Error"
            await status_msg.edit_text(f"❌ Real-ESRGAN failed on chunk {i+1}/{total}.\n\n<b>Error details:</b>\n<code>{err_snippet}</code>", parse_mode=enums.ParseMode.HTML)
            return None
        
        await status_msg.edit_text(
            f"☁️ <b>Auto-saving Chunk {i+1}/{total}</b> to DB Channel...",
            parse_mode=enums.ParseMode.HTML
        )
        try:
            if os.path.isdir(out_path):
                vids = glob.glob(os.path.join(out_path, "*.mp4")) + glob.glob(os.path.join(out_path, "*.mkv"))
                if vids:
                    out_path = vids[0]
            safe_path = f"/content/upload_chunk_{i}.mp4"
            import shutil
            shutil.copy(out_path, safe_path)
            await client.send_document(
                chat_id=PRIVATE_DB_CHANNEL,
                document=safe_path,
                caption=f"🎬 4K Chunk | {basename}\n{chunk_tag}\n#{job_tag}",
                force_document=True
            )
            if os.path.exists(safe_path): os.remove(safe_path)
        except Exception as e:
            logger.error(f"Failed to upload chunk {i} to DB channel: {e}")
        
        final_chunks.append(out_path)
    
    # Step 4: Merge
    await status_msg.edit_text(
        f"🔄 <b>Step 4/4:</b> Merging {total} chunks into final 4K video...",
        parse_mode=enums.ParseMode.HTML
    )
    
    merged_path = f"/content/{basename}_4K.mp4"
    merge_ok = await _merge_chunks(final_chunks, merged_path)
    if not merge_ok:
        await status_msg.edit_text("❌ Failed to merge chunks.")
        return None
    
    # Encode to 10-bit HEVC x265
    await status_msg.edit_text("🎬 <b>Encoding to 10-bit HEVC x265...</b>", parse_mode=enums.ParseMode.HTML)
    hevc_path = f"/content/{basename}_4K_HEVC.mkv"
    hevc_ok = await _encode_hevc_10bit(merged_path, hevc_path)
    
    final_output = hevc_path if hevc_ok else merged_path
    
    # Cleanup work dir
    shutil.rmtree(work_dir, ignore_errors=True)
    if hevc_ok and os.path.exists(merged_path):
        os.remove(merged_path)
    
    return final_output

async def handle_enhance(client, message):
    """Handler for /enhance command - 4K upscale with auto-resume"""
    # Get the video to enhance
    video_path = None
    
    # Case 1: /enhance as reply to a video/document
    if message.reply_to_message and (message.reply_to_message.video or message.reply_to_message.document):
        status = await message.reply("⬇️ <b>Downloading video from Telegram...</b>", parse_mode=enums.ParseMode.HTML)
        try:
            video_path = await client.download_media(message.reply_to_message, file_name="/content/enhance_input/")
        except Exception as e:
            return await status.edit_text(f"❌ Download failed: {e}")
    
    # Case 2: /enhance <magnet_or_url>
    elif len(message.command) >= 2:
        link = " ".join(message.command[1:])  # Support links with spaces
        dl_dir = "/content/enhance_input"
        os.makedirs(dl_dir, exist_ok=True)
        status = await message.reply("⬇️ <b>Downloading...</b>", parse_mode=enums.ParseMode.HTML)
        
        try:
            if link.startswith("magnet:"):
                # Magnet links: use aria2c CLI directly for proper torrent handling
                await status.edit_text("🧲 <b>Fetching torrent metadata...</b>", parse_mode=enums.ParseMode.HTML)
                cmd = [
                    "aria2c", "--seed-time=0", "--max-concurrent-downloads=5",
                    "--dir", dl_dir, "--console-log-level=error",
                    "--summary-interval=0", link
                ]
                proc = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await status.edit_text("⬇️ <b>Downloading torrent content...</b>\n<i>This may take a while...</i>", parse_mode=enums.ParseMode.HTML)
                await proc.communicate()
                
                # Find the largest video file in the download directory
                VIDEO_EXTS = {'.mp4', '.mkv', '.avi', '.webm', '.mov', '.flv', '.wmv', '.ts', '.m4v'}
                largest_file = None
                largest_size = 0
                for root, dirs, files in os.walk(dl_dir):
                    for f in files:
                        ext = os.path.splitext(f)[1].lower()
                        if ext in VIDEO_EXTS:
                            fpath = os.path.join(root, f)
                            fsize = os.path.getsize(fpath)
                            if fsize > largest_size:
                                largest_size = fsize
                                largest_file = fpath
                
                video_path = largest_file
                if video_path:
                    size_mb = largest_size / (1024 * 1024)
                    await status.edit_text(f"✅ <b>Downloaded!</b> Found: <code>{os.path.basename(video_path)}</code> ({size_mb:.0f} MB)", parse_mode=enums.ParseMode.HTML)
            else:
                # Direct URL: use aria2_api
                global aria2_api
                dl = aria2_api.add_uris([link], options={"dir": dl_dir})
                while not dl.is_complete:
                    await asyncio.sleep(3)
                    dl.update()
                    if dl.status == "error":
                        return await status.edit_text(f"❌ Download error: {dl.error_message}")
                    try:
                        pct = dl.progress_string()
                        speed = dl.download_speed_string()
                        await status.edit_text(f"⬇️ Downloading... {pct} | {speed}", parse_mode=enums.ParseMode.HTML)
                    except Exception:
                        pass
                video_path = dl.files[0].path if dl.files else None
        except Exception as e:
            return await status.edit_text(f"❌ Download failed: {e}")
    else:
        return await message.reply(
            "📖 <b>4K Enhance - Usage:</b>\n\n"
            "• Reply to a video: <code>/enhance</code>\n"
            "• Direct link/magnet: <code>/enhance &lt;url_or_magnet&gt;</code>\n\n"
            "Bot will split → upscale 4K → auto-save to DB channel → merge & send.",
            parse_mode=enums.ParseMode.HTML
        )
    
    if not video_path or not os.path.exists(video_path):
        return await status.edit_text("❌ No video file found after download.\n\n<i>Tip: Make sure the torrent/link contains a video file (.mp4, .mkv, etc.)</i>", parse_mode=enums.ParseMode.HTML)
    
    # --- Begin 4K Enhancement Pipeline ---
    basename = os.path.splitext(os.path.basename(video_path))[0]
    job_tag = f"{basename}_{model}".replace(" ", "_").replace(".", "_").replace("-", "_")
    
    work_dir = f"/content/4k_work_{job_tag}"
    raw_dir = os.path.join(work_dir, "raw_chunks")
    upscaled_dir = os.path.join(work_dir, "4k_chunks")
    os.makedirs(raw_dir, exist_ok=True)
    os.makedirs(upscaled_dir, exist_ok=True)
    
    # Step 1: Split
    await status.edit_text("✂️ <b>Step 1/4:</b> Splitting video into 1-min chunks...", parse_mode=enums.ParseMode.HTML)
    raw_chunks = await _split_video_to_chunks(video_path, raw_dir, segment_seconds=60)
    total = len(raw_chunks)
    if total == 0:
        return await status.edit_text("❌ FFmpeg split failed - no chunks created.")
    
    await status.edit_text(f"✂️ Split into <b>{total}</b> chunks.", parse_mode=enums.ParseMode.HTML)
    
    # Step 2: Resume check
    await status.edit_text("🔍 <b>Step 2/4:</b> Checking DB Channel for previous progress...", parse_mode=enums.ParseMode.HTML)
    existing = await _find_completed_chunks(client, job_tag)
    skipped = 0
    
    # Step 3: Upscale each chunk
    final_chunks = []
    for i, raw_chunk in enumerate(raw_chunks):
        chunk_tag = f"#{job_tag}_Chunk_{i:03d}"
        out_name = os.path.basename(raw_chunk).replace(".mp4", "_4k.mp4")
        out_path = os.path.join(upscaled_dir, out_name)
        
        # Check if this chunk was already done in a previous session
        if chunk_tag in existing:
            skipped += 1
            await status.edit_text(
                f"⏩ <b>Chunk {i+1}/{total}</b> — already in DB! Downloading back...\n"
                f"(Skipped: {skipped} | Remaining: {total - i - 1})",
                parse_mode=enums.ParseMode.HTML
            )
            try:
                await client.download_media(existing[chunk_tag], file_name=out_path)
            except Exception as e:
                logger.error(f"Failed to download chunk {i} from DB: {e}")
                # If download fails, re-process it
                await status.edit_text(f"⚠️ DB download failed for chunk {i+1}, re-processing...")
                msg_text = f"⚠️ DB download failed for chunk {i+1}, re-processing..."
                success, err_msg = await _upscale_chunk_realesrgan(raw_chunk, out_path, status, msg_text)
                if not success:
                    return await status.edit_text(f"❌ Upscaling failed at chunk {i+1}.")
            final_chunks.append(out_path)
            continue
        
        # Process this chunk fresh
        msg_text = f"🚀 <b>Step 3/4: Upscaling Chunk {i+1}/{total}</b> to 4K...\n⏩ Skipped: {skipped} | ⏳ Remaining after this: {total - i - 1}\n\n<i>This may take 15-40 min per chunk depending on GPU.</i>"
        await status.edit_text(msg_text, parse_mode=enums.ParseMode.HTML)
        
        success, err_msg = await _upscale_chunk_realesrgan(raw_chunk, out_path, status, msg_text)
        if not success:
            return await status.edit_text(f"❌ Real-ESRGAN failed on chunk {i+1}/{total}.")
        
        # Upload completed chunk to Private DB Channel (auto-save)
        await status.edit_text(
            f"☁️ <b>Auto-saving Chunk {i+1}/{total}</b> to DB Channel...",
            parse_mode=enums.ParseMode.HTML
        )
        try:
            if os.path.isdir(out_path):
                vids = glob.glob(os.path.join(out_path, "*.mp4")) + glob.glob(os.path.join(out_path, "*.mkv"))
                if vids:
                    out_path = vids[0]
            safe_path = f"/content/upload_chunk_{i}.mp4"
            import shutil
            shutil.copy(out_path, safe_path)
            await client.send_document(
                chat_id=PRIVATE_DB_CHANNEL,
                document=safe_path,
                caption=f"🎬 4K Chunk | {basename}\n{chunk_tag}\n#{job_tag}",
                force_document=True
            )
            if os.path.exists(safe_path): os.remove(safe_path)
        except Exception as e:
            logger.error(f"Failed to upload chunk {i} to DB channel: {e}")
            # Don't abort - we still have the local file
        
        final_chunks.append(out_path)
    
    # Step 4: Merge all 4K chunks
    await status.edit_text(
        f"🔄 <b>Step 4/4:</b> Merging {total} chunks into final 4K video...",
        parse_mode=enums.ParseMode.HTML
    )
    
    merged_path = f"/content/{basename}_4K.mp4"
    merge_ok = await _merge_chunks(final_chunks, merged_path)
    if not merge_ok:
        return await status.edit_text("❌ Failed to merge chunks.")
    
    # Optional: Encode to 10-bit HEVC x265
    await status.edit_text("🎬 <b>Encoding to 10-bit HEVC x265...</b>", parse_mode=enums.ParseMode.HTML)
    hevc_path = f"/content/{basename}_4K_HEVC.mkv"
    hevc_ok = await _encode_hevc_10bit(merged_path, hevc_path)
    
    final_output = hevc_path if hevc_ok else merged_path
    
    # Upload final video to user
    await status.edit_text("📤 <b>Uploading final 4K video...</b>", parse_mode=enums.ParseMode.HTML)
    try:
        file_size = os.path.getsize(final_output)
        if file_size > MAX_FILE_SIZE:
            await status.edit_text(
                f"⚠️ Final video is {file_size // (1024*1024)} MB (over 2GB limit).\n"
                f"Uploading to DB Channel instead...",
                parse_mode=enums.ParseMode.HTML
            )
            await client.send_document(chat_id=PRIVATE_DB_CHANNEL, document=final_output,
                caption=f"🎬 4K FINAL | {basename}\n#{job_tag}_FINAL", force_document=True)
            await status.edit_text("✅ <b>Done!</b> Final 4K video saved to DB Channel (too large for chat).")
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
        await status.edit_text(f"❌ Upload failed: {e}")
    
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







