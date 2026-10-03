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
















































































































































































































































































































































































































































































































































































































































































































































        cmd = ["video2x", "--input", input_path, "--output", output_path, "--processor", "realesrgan", "--realesrgan-model", "realesr-animevideov3", "--scaling-factor", "4"]
    elif model == "cugan":
        tmp_in = output_path + "_tmp_in"
        tmp_out = output_path + "_tmp_out"
        bash_cmd = (
            f"mkdir -p '{tmp_in}' '{tmp_out}' && "
            f"ffmpeg -hide_banner -loglevel error -i '{input_path}' '{tmp_in}/%08d.jpg' && "
            f"cd /content/realcugan && ./realcugan-ncnn-vulkan -i '{tmp_in}' -o '{tmp_out}' -s 2 -n 2 -f jpg && "
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
            f"cd /content/realesrgan_vulkan && ./realesrgan-ncnn-vulkan -i '{tmp_in}' -o '{tmp_out}' -n realesrgan-x4plus-anime -s 4 -f jpg && "
            f"FPS=$(ffprobe -v error -select_streams v:0 -show_entries stream=r_frame_rate -of default=noprint_wrappers=1:nokey=1 '{input_path}') && "
            f"ffmpeg -hide_banner -loglevel error -framerate $FPS -i '{tmp_out}/%08d.jpg' -i '{input_path}' -map 0:v -map 1:a? -c:v libx264 -crf 20 -c:a copy '{output_path}' && "
            f"rm -rf '{tmp_in}' '{tmp_out}'"
        )
        cmd = ["bash", "-c", bash_cmd]
    else:


            "python", "/content/Real-ESRGAN/inference_realesrgan_video.py",


            "-i", input_path,


            "--outscale", "2" if model == "v3" else "4"























            match = re.search(r'(\d+)/\d+', clean) if not matches else True
                clean = text.strip()  # preserve text for display
            now = time.time()
            if match:
                current_frame = int(matches[-1]) if "matches" in locals() and matches else int(match.group(1))
                if current_frame - last_frame >= 1 or now - last_update > 8:
                    last_frame = current_frame
                    if clean:


                            progress_callback(clean)


                            try:
                                await status_msg.edit_text(f"{prefix_text}\n\n⏳ <b>AI Processing:</b>\n<code>{clean}</code>", parse_mode=enums.ParseMode.HTML)
                                last_update = now
                            except: pass
            else:
                if now - last_update > 8:
                    if clean:


                            progress_callback(clean)


                            try:
                                await status_msg.edit_text(f"{prefix_text}\n\n⏳ <b>AI Processing:</b>\n<code>{clean}</code>", parse_mode=enums.ParseMode.HTML)
                                last_update = now
                            except: pass

    await proc.wait()
    
    if model not in ["anime4k", "cugan"]:
        import shutil
        basename = os.path.splitext(os.path.basename(input_path))[0]
        actual_file = os.path.join(output_path, f"{basename}_out.mp4")
        if os.path.exists(actual_file):
            tmp_dir = output_path + "_dir"
            os.rename(output_path, tmp_dir)
            shutil.move(os.path.join(tmp_dir, f"{basename}_out.mp4"), output_path)
            shutil.rmtree(tmp_dir, ignore_errors=True)

    return proc.returncode == 0 and os.path.exists(output_path), err_acc

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







