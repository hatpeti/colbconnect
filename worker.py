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

MASTER_WS_URL = "wss://leech-production-214b.up.railway.app"
TARGET_CHANNEL = "@animedubsinhla"
WATERMARK = "@animesinhala1"

app = None
user_app = None
upload_client = None
aria2_api = None

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

    cmd = ["ffmpeg", "-y", "-hwaccel", "auto", "-i", input_path, "-vf", scale, *vcodec, "-c:a", "copy", "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-c:s", "copy", "-progress", "pipe:1", "-nostats", "-loglevel", "error", output_path]
    return await run_ffmpeg_operation(cmd, input_path, output_path, total_duration, task_id, action, filename)

# --- UPLOAD OPERATIONS (DOCUMENT / FORCE FILE) ---
async def upload_part(client, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event):
    offset = part_no * PART_SIZE
    size_to_read = min(PART_SIZE, file_size - offset)
    if size_to_read <= 0: return 0
    data = await asyncio.to_thread(os.pread, fd, size_to_read, offset)
    if cancel_event.is_set(): raise asyncio.CancelledError
    
    last_error = None
    for attempt in range(1, PART_RETRIES + 1):
        if cancel_event.is_set(): raise asyncio.CancelledError
        try:
            if is_big: result = await client.invoke(raw.functions.upload.SaveBigFilePart(file_id=file_id, file_part=part_no, file_total_parts=total_parts, bytes=data))
            else: result = await client.invoke(raw.functions.upload.SaveFilePart(file_id=file_id, file_part=part_no, bytes=data))
            if result: return len(data)
        except FloodWait as e:
            await asyncio.sleep(min(int(getattr(e, "value", 0)), 300))
        except Exception as e:
            last_error = e
            if attempt >= PART_RETRIES: break
            await asyncio.sleep(min(2.0, 0.15 * (2 ** (attempt - 1))))
    raise RuntimeError(f"Part {part_no} failed: {last_error}")

async def parallel_upload_file(path, filename, task_id):
    global upload_client
    file_size = os.path.getsize(path)
    is_big = file_size > BIG_FILE_THRESHOLD
    total_parts = math.ceil(file_size / PART_SIZE)
    worker_count = min(UPLOAD_WORKERS, total_parts)
    cancel_event = asyncio.Event()

    runtime = {"cancel_event": cancel_event, "upload_tasks": set(), "progress_task": None}
    upload_runtime[task_id] = runtime
    file_id = random.randint(-2**63, 2**63 - 1)
    queue = asyncio.Queue()
    for p in range(total_parts): queue.put_nowait(p)

    state = {"uploaded": 0, "lock": asyncio.Lock(), "last_bytes": 0, "last_time": time.time()}
    start_time = time.time()

    if task_id in ACTIVE_TASKS:
        ACTIVE_TASKS[task_id].update({
            "status": "📤 Uploading Document",
            "filename": filename,
            "current": 0,
            "total": file_size,
            "is_time": False,
            "start_time": start_time,
            "speed": 0,
            "eta": 0
        })

    async def progress_pump():
        while not cancel_event.is_set():
            await asyncio.sleep(1.0)
            current = state["uploaded"]
            now = time.time()
            elapsed = max(now - start_time, 0.001)
            speed = current / elapsed
            eta = max(file_size - current, 0) / speed if speed > 0 else 0
            if task_id in ACTIVE_TASKS:
                ACTIVE_TASKS[task_id].update({
                    "current": current,
                    "speed": speed,
                    "eta": eta
                })
            if current >= file_size: break

    runtime["progress_task"] = asyncio.create_task(progress_pump())
    fd = os.open(path, os.O_RDONLY)

    async def worker():
        while not cancel_event.is_set():
            try: part_no = queue.get_nowait()
            except: return
            try:
                sent_bytes = await upload_part(upload_client, fd, file_id, part_no, total_parts, file_size, is_big, cancel_event)
                async with state["lock"]: state["uploaded"] += sent_bytes
            except:
                cancel_event.set()
                raise
            finally: queue.task_done()

    try:
        tasks = [asyncio.create_task(worker()) for _ in range(worker_count)]
        runtime["upload_tasks"] = set(tasks)
        await asyncio.gather(*tasks, return_exceptions=True)
        if cancel_event.is_set(): raise asyncio.CancelledError
        if is_big: input_file = raw.types.InputFileBig(id=file_id, parts=total_parts, name=filename)
        else: input_file = raw.types.InputFile(id=file_id, parts=total_parts, name=filename, md5_checksum="")
        return input_file
    finally:
        cancel_event.set()
        with contextlib.suppress(Exception): os.close(fd)
        if runtime.get("progress_task"): runtime["progress_task"].cancel()
        upload_runtime.pop(task_id, None)

async def send_uploaded_media(chat_id, input_file, filename, thumb_path, caption):
    sender = upload_client
    thumb = None
    if thumb_path and os.path.exists(thumb_path):
        with contextlib.suppress(Exception):
            thumb = await sender.save_file(thumb_path)

    # Force document / file upload (no streaming flag)
    media = raw.types.InputMediaUploadedDocument(
        file=input_file,
        thumb=thumb,
        mime_type=get_mime_type(filename),
        attributes=[raw.types.DocumentAttributeFilename(file_name=filename)],
        force_file=True
    )

    msg_id = None
    # 1. Send first to chat_id (source user or group)
    try:
        peer = await sender.resolve_peer(chat_id)
        parsed = await sender.parser.parse(caption or f"<code>{filename}</code>", enums.ParseMode.HTML)
        res = await sender.invoke(
            raw.functions.messages.SendMedia(
                peer=peer,
                media=media,
                message=parsed.get("message", ""),
                entities=parsed.get("entities", None),
                random_id=sender.rnd_id()
            )
        )
        for update in getattr(res, "updates", []):
            m = getattr(update, "message", None)
            if m and getattr(m, "id", None):
                msg_id = m.id
                break
        if not msg_id and hasattr(res, "id"):
            msg_id = res.id
        logger.info(f"Successfully delivered to chat {chat_id} (msg_id: {msg_id})")
    except Exception as e:
        logger.error(f"Error delivering to chat {chat_id}: {e}")

    # 2. Automatically copy to Database Channel TARGET_CHANNEL (@animedubsinhla)
    if str(chat_id).lower() != TARGET_CHANNEL.lower() and msg_id:
        try:
            await sender.copy_message(
                chat_id=TARGET_CHANNEL,
                from_chat_id=chat_id,
                message_id=msg_id,
                caption=caption or f"<code>{filename}</code>",
                parse_mode=enums.ParseMode.HTML
            )
            logger.info(f"Successfully copied to database channel {TARGET_CHANNEL}")
        except Exception as e:
            logger.error(f"Error copying to channel {TARGET_CHANNEL}: {e}")
            if "CHAT_WRITE_FORBIDDEN" in str(e) or "CHANNEL_PRIVATE" in str(e):
                with contextlib.suppress(Exception):
                    await sender.send_message(
                        chat_id,
                        f"⚠️ <b>Database Warning:</b> Bot cannot post to <code>{TARGET_CHANNEL}</code>.\n"
                        f"Please add the bot as an <b>Admin with 'Post Messages' permission</b> to the channel!",
                        parse_mode=enums.ParseMode.HTML
                    )

# --- UI MENUS ---
def get_panel_markup(task_id):
    def btn(text, data, style=enums.ButtonStyle.DEFAULT):
        return InlineKeyboardButton(text, callback_data=data, style=style)

    return InlineKeyboardMarkup([
        [btn("480p", f"panel_480_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("720p", f"panel_720_{task_id}", style=enums.ButtonStyle.PRIMARY),
         btn("1080p", f"panel_1080_{task_id}", style=enums.ButtonStyle.PRIMARY)],
        [btn("✂️ Remove Sub", f"panel_removesub_{task_id}", style=enums.ButtonStyle.DEFAULT),
         btn("📝 Add Sub", f"panel_addsub_{task_id}", style=enums.ButtonStyle.DEFAULT)],
        [btn("🔄 Re-encode All", f"panel_reencode_{task_id}", style=enums.ButtonStyle.PRIMARY)],
        [btn("🚀 Upload Now", f"panel_upload_{task_id}", style=enums.ButtonStyle.SUCCESS)],
        [btn("❌ Cancel", f"panel_cancel_{task_id}", style=enums.ButtonStyle.DANGER)]
    ])

# Sinhala Help Menu
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
    if file_index in custom_renames: filename = custom_renames[file_index]
    
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
        out_path = os.path.join(os.path.dirname(filepath), os.path.splitext(filename)[0] + ".srt")
        cmd = ["ffmpeg", "-y", "-i", filepath, "-map", "0:s:0", out_path]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        if os.path.exists(out_path): files_to_upload.append(out_path)
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

    # 2. Upload generated files as Documents (Force File)
    for f_path in files_to_upload:
        if cancel_flags.get(task_id): break
        f_name = os.path.basename(f_path)
        input_file = await parallel_upload_file(f_path, f_name, task_id)
        if cancel_flags.get(task_id): break
        await send_uploaded_media(chat_id, input_file, f_name, CUSTOM_THUMB_PATH, f"<code>{f_name}</code>")

# --- EXECUTE TASK WORKER WITH SEMAPHORE ---
async def execute_task_worker(client, chat_id, task_id, action, media_msg=None, is_telegram_file=False, sub_path=None, audio_path=None):
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

                path = await client.download_media(media_msg, file_name=path, progress=tg_progress)
                await process_media_file(client, chat_id, path, action, {}, 1, task_id, sub_path, audio_path)
                shutil.rmtree(dl_dir, ignore_errors=True)

            elif not is_telegram_file and task_id in current_tasks:
                t_data = current_tasks.pop(task_id)
                dl_dir = f"/content/dl_{task_id}"
                os.makedirs(dl_dir, exist_ok=True)
                global aria2_api
                dl = aria2_api.add_torrent(t_data["torrent"], options={"dir": dl_dir, "select-file": ",".join(map(str, t_data["selected"]))})
                
                ACTIVE_TASKS[task_id] = {
                    "task_id": task_id,
                    "chat_id": chat_id,
                    "user_mention": t_data.get("user_mention", "User"),
                    "filename": t_data["t_name"],
                    "status": "📥 Downloading Torrent",
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
                        
                if not cancel_flags.get(task_id):
                    for f in dl.files:
                        if getattr(f, "selected", False) and os.path.exists(str(f.path)):
                            await process_media_file(client, chat_id, str(f.path), action, {}, f.index, task_id)
                shutil.rmtree(dl_dir, ignore_errors=True)

        except asyncio.CancelledError:
            logger.info(f"Task {task_id} was cancelled.")
        except Exception as e:
            logger.error(f"Task {task_id} error: {e}")
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

# --- REPLY COMMANDS & FILE SELECTION ---
async def reply_handler(client, message):
    if not message.reply_to_message: return
    r_id = str(message.reply_to_message.id)
    text = (message.text or message.caption or "").strip().lower()
    
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
            elif action in ["480", "720", "1080", "reencode", "removesub", "extract_sub", "extract_audio", "remaudio", "extract_thumb"]:
                task_id = str(message.id)
                asyncio.create_task(execute_task_worker(client, message.chat.id, task_id, action, media_msg=message.reply_to_message, is_telegram_file=True))

# --- PANEL CALLBACKS ---
async def cb_handler(client, cb):
    data = cb.data
    if data.startswith("panel_"):
        parts = data.split("_", 2)
        action, task_id = parts[1], parts[2]
        
        if action == "cancel":
            cancel_flags[task_id] = True
            return await cb.message.edit_text("🚫 <b>Task cancelled.</b>", parse_mode=enums.ParseMode.HTML)
            
        if action == "addsub":
            await cb.message.edit_reply_markup(None)
            prompt = await cb.message.reply("📝 <b>Please reply to THIS message with your subtitle file (.srt, .ass, etc.)</b>", parse_mode=enums.ParseMode.HTML)
            pending_sub_replies[str(prompt.id)] = cb.message.reply_to_message
            return

        await cb.message.edit_reply_markup(None)
        is_tg = (task_id not in current_tasks)
        media_target = cb.message.reply_to_message
        if not media_target and is_tg:
            try:
                media_target = await client.get_messages(cb.message.chat.id, int(task_id))
            except:
                pass
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

# --- START COMMAND ---
async def start_cmd(client, message):
    await message.reply(
        "⚡ <b>Colab Worker 3.0 is Online & Ready!</b>\n\n"
        "• Use <code>/leech &lt;magnet_link&gt;</code> to download torrents.\n"
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
            app.add_handler(MessageHandler(panel_cmd, filters.command(["panel", "encode"])))
            app.add_handler(MessageHandler(handle_leech, filters.command("leech")))
            app.add_handler(MessageHandler(cancel_cmd, filters.regex(r"^/cancel")))
            app.add_handler(MessageHandler(handle_telegram_file, (filters.document | filters.video)))
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
