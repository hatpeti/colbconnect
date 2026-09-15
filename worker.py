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
app = None
user_app = None
upload_client = None

# Settings
PART_SIZE = 512 * 1024
UPLOAD_WORKERS = 8
PART_RETRIES = 8
UI_INTERVAL = 1.5
MAX_FILE_SIZE = 2000 * 1024 * 1024
BIG_FILE_THRESHOLD = 10 * 1024 * 1024
SESSION_DIR = "/content/telegram_sessions"
os.makedirs(SESSION_DIR, exist_ok=True)
THUMB_DIR = "/content/bot_thumbnail"
os.makedirs(THUMB_DIR, exist_ok=True)
CUSTOM_THUMB_PATH = os.path.join(THUMB_DIR, "custom_thumb.jpg")

current_tasks = {}
cancel_flags = {}
upload_runtime = {}
batch_states = {}

# --- HELPERS ---
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

def safe_html(text): return html.escape(str(text), quote=False)

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

def get_mime_type(path): return mimetypes.guess_type(path)[0] or "application/octet-stream"

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

async def apply_mkv_watermark(filepath, watermark="@animesinhala1"):
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

# --- ENCODING & FFMPEG OPERATIONS ---
async def update_ui(message, action, filename, current, total, start_time, last_edit_time, task_id=None, force=False, extra_lines=None, is_encoding=False):
    now = time.time()
    if not force and (now - last_edit_time[0] < UI_INTERVAL) and current < total: return last_edit_time[0]
    last_edit_time[0] = now
    percentage = (current / total) * 100 if total > 0 else 0
    filled = min(18, int(18 * percentage / 100))
    bar = "█" * filled + "░" * (18 - filled)
    elapsed = max(now - start_time, 0.001)
    speed = current / elapsed
    eta = max(total - current, 0) / speed if speed > 0 else 0

    msg = f"<b>🎬 {safe_html(filename)}</b>\n\n<b>ක්‍රියාවලිය:</b> {safe_html(action)}\n<code>[{bar}] {percentage:.1f}%</code>\n"
    if is_encoding:
        msg += f"<i>{format_time(current)} / {format_time(total)}</i>\n<b>Speed:</b> {speed:.2f}x\n<b>ETA:</b> {format_time(eta)}\n"
    else:
        msg += f"<i>{format_bytes(current)} / {format_bytes(total)}</i>\n<b>වේගය:</b> {format_bytes(speed)}/s ({format_mbps(speed)})\n<b>ETA:</b> {format_time(eta)}\n"

    if extra_lines: msg += "\n" + "\n".join(extra_lines) + "\n"
    markup = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Cancel Task", callback_data=f"stop_{task_id}")]]) if task_id else None
    try: await message.edit_text(msg, parse_mode=enums.ParseMode.HTML, reply_markup=markup)
    except: pass
    return last_edit_time[0]

async def run_ffmpeg_operation(cmd, input_path, output_path, total_duration, ui_msg, task_id, action_name, filename):
    process = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    start_time, last_edit_time = time.time(), [0]
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
                await update_ui(ui_msg, action_name, filename, max(0, time_us / 1_000_000), max(total_duration, 1), start_time, last_edit_time, task_id, is_encoding=True)
            except: pass
    await process.wait()
    if process.returncode != 0:
        err = await process.stderr.read()
        raise Exception(f"FFMPEG Failed:\n{err.decode('utf-8', errors='ignore')[-1000:]}")
    return os.path.exists(output_path)

async def encode_video(input_path, output_path, resolution, total_duration, ui_msg, task_id, filename):
    has_nvenc = check_gpu()
    if resolution == 1080: scale, cq, crf = "scale=-2:'min(1080,ih)'", "30", "28"
    elif resolution == 720: scale, cq, crf = "scale=-2:'min(720,ih)'", "34", "32"
    else: scale, cq, crf = "scale=-2:'min(480,ih)'", "38", "36"

    if has_nvenc:
        vcodec = ["-c:v", "h264_nvenc", "-preset", "p6", "-tune", "hq", "-cq", cq, "-pix_fmt", "yuv420p"]
        action = f"⚙️ Re-Encoding {resolution}p (GPU)"
    else:
        vcodec = ["-c:v", "libx264", "-preset", "veryfast", "-crf", crf, "-pix_fmt", "yuv420p"]
        action = f"⚙️ Re-Encoding {resolution}p (CPU)"

    cmd = ["ffmpeg", "-y", "-hwaccel", "auto", "-i", input_path, "-vf", scale, *vcodec, "-c:a", "copy", "-map", "0:v:0", "-map", "0:a?", "-map", "0:s?", "-c:s", "copy", "-progress", "pipe:1", "-nostats", "-loglevel", "error", output_path]
    return await run_ffmpeg_operation(cmd, input_path, output_path, total_duration, ui_msg, task_id, action, filename)

# --- UPLOAD ---
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

async def parallel_upload_file(path, filename, task_id, ui_msg):
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

    state = {"uploaded": 0, "lock": asyncio.Lock(), "last_bytes": 0, "last_time": time.time(), "instant": 0.0}
    start_time, last_edit_time = time.time(), [0]

    async def progress_pump():
        while not cancel_event.is_set():
            await asyncio.sleep(0.5)
            current = state["uploaded"]
            now = time.time()
            dt = max(now - state["last_time"], 0.001)
            state["instant"] = max(0.0, (current - state["last_bytes"]) / dt)
            state["last_bytes"] = current
            state["last_time"] = now
            if current >= file_size: break
            avg = current / max(now - start_time, 0.001)
            await update_ui(ui_msg, "📤 Telegram Upload", filename, current, file_size, start_time, last_edit_time, task_id, extra_lines=[f"<b>⚡ Instant:</b> {format_bytes(state['instant'])}/s", f"<b>AVG:</b> {format_bytes(avg)}/s"])

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
        avg = file_size / max(time.time() - start_time, 0.001)
        await update_ui(ui_msg, f"✅ Upload complete | AVG {format_bytes(avg)}/s", filename, file_size, file_size, start_time, last_edit_time, task_id, force=True)
        return input_file
    finally:
        cancel_event.set()
        with contextlib.suppress(Exception): os.close(fd)
        if runtime.get("progress_task"): runtime["progress_task"].cancel()
        upload_runtime.pop(task_id, None)

async def send_uploaded_media(chat_id, input_file, filename, is_video, duration, width, height, thumb_path, caption):
    sender = upload_client
    thumb = None
    if thumb_path and os.path.exists(thumb_path):
        with contextlib.suppress(Exception): thumb = await sender.save_file(thumb_path)

    if is_video:
        mime = "video/mp4" if filename.lower().endswith(".mp4") else "video/x-matroska"
        attr = [raw.types.DocumentAttributeVideo(duration=int(duration or 1), w=int(width or 1280), h=int(height or 720), supports_streaming=True), raw.types.DocumentAttributeFilename(file_name=filename)]
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=mime, attributes=attr, force_file=None)
    else:
        media = raw.types.InputMediaUploadedDocument(file=input_file, thumb=thumb, mime_type=get_mime_type(filename), attributes=[raw.types.DocumentAttributeFilename(file_name=filename)], force_file=True)

    peer = await sender.resolve_peer(chat_id)
    parsed = await sender.parser.parse(caption or "", enums.ParseMode.HTML)
    return await sender.invoke(raw.functions.messages.SendMedia(peer=peer, media=media, message=parsed.get("message", ""), entities=parsed.get("entities", None), random_id=sender.rnd_id()))

# --- UI MENUS ---
def get_panel_markup(task_id):
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🎬 480p", callback_data=f"panel_480_{task_id}"),
         InlineKeyboardButton("🎬 720p", callback_data=f"panel_720_{task_id}"),
         InlineKeyboardButton("🎬 1080p", callback_data=f"panel_1080_{task_id}")],
        [InlineKeyboardButton("✂️ Remove Sub", callback_data=f"panel_removesub_{task_id}"),
         InlineKeyboardButton("📝 Add Sub", callback_data=f"panel_addsub_{task_id}")],
        [InlineKeyboardButton("🔄 Re-encode", callback_data=f"panel_reencode_{task_id}")],
        [InlineKeyboardButton("📤 Upload Now", callback_data=f"panel_upload_{task_id}")],
        [InlineKeyboardButton("❌ Cancel", callback_data=f"panel_cancel_{task_id}")]
    ])

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
        "<code>/addsub</code> - Subtitle එකතු කිරීම.\n"
        "<code>/extract_sub</code> - Subtitle එක වෙනම ගලවාගැනීම.\n\n"
        "<b>🎵 Audio:</b>\n"
        "<code>/addaudio</code> - අලුත් Audio Track එකක් දැමීම.\n"
        "<code>/extract_audio</code> - Audio එක වෙනම ගලවාගැනීම.\n"
        "<code>/remaudio</code> - Audio Track එක අයින් කිරීම.\n\n"
        "<b>🖼 Thumbnail:</b>\n"
        "<code>/extract_thumb</code> - වීඩියෝ එකේ Thumbnail එක ගලවාගැනීම."
    )
    await message.reply_text(text, parse_mode=enums.ParseMode.HTML)

# --- PROCESS MEDIA PIPELINE ---
async def process_media_file(client, chat_id, filepath, action, custom_renames, file_index, ui_msg, task_id):
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
            success = await encode_video(filepath, out_path, res, duration, ui_msg, task_id, out_file)
            if success:
                out_path = await apply_mkv_watermark(out_path)
                files_to_upload.append(out_path)
    elif action == "removesub":
        out_path = filepath + "_nosub.mkv"
        cmd = ["ffmpeg", "-y", "-i", filepath, "-map", "0:v", "-map", "0:a?", "-c", "copy", out_path]
        success = await run_ffmpeg_operation(cmd, filepath, out_path, duration, ui_msg, task_id, "✂️ Removing Subs", filename)
        if success: files_to_upload.append(await apply_mkv_watermark(out_path))
    else:
        # Default upload (Upload Now or unhandled actions for now)
        files_to_upload.append(filepath)

    # 2. Upload generated files
    for f_path in files_to_upload:
        if cancel_flags.get(task_id): break
        f_name = os.path.basename(f_path)
        dur, width, height = await get_video_meta(f_path)
        input_file = await parallel_upload_file(f_path, f_name, task_id, ui_msg)
        if cancel_flags.get(task_id): break
        await send_uploaded_media(chat_id, input_file, f_name, True, dur, width, height, CUSTOM_THUMB_PATH, f"<code>{f_name}</code>")

    if not cancel_flags.get(task_id):
        await ui_msg.edit_text("<b>✅ ක්‍රියාවලිය සාර්ථකව අවසන්!</b>", parse_mode=enums.ParseMode.HTML)

# --- TELEGRAM FILE HANDLER (PANEL TRIGGER) ---
async def handle_telegram_file(client, message):
    if not (message.document or message.video): return
    file_name = getattr(message.document or message.video, "file_name", "unknown_file.mp4")
    task_id = str(message.id)
    
    await message.reply_text(
        f"<b>✅ ගොනුව ලැබුණා!</b>\n<code>{safe_html(file_name)}</code>\n\n"
        "👉 කරුණාකර පහත Encoding Panel එකෙන් අවශ්‍ය ක්‍රියාව තෝරන්න:",
        reply_markup=get_panel_markup(task_id),
        parse_mode=enums.ParseMode.HTML,
        quote=True
    )

# --- LEECH COMMAND ---
async def handle_leech(client, message):
    if len(message.command) < 2:
        return await message.reply("Please provide a magnet link! Example: `/leech magnet:?...`")
    magnet = message.command[1]
    
    status_msg = await message.reply("🔍 ටොරන්ට් දත්ත ලබාගනිමින් පවතී...")
    temp_dir = f"/content/meta_{message.id}"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        cmd = ["aria2c", "--bt-metadata-only=true", "--bt-save-metadata=true", "--console-log-level=error", "--dir", temp_dir, magnet]
        proc = await asyncio.create_subprocess_exec(*cmd)
        await proc.communicate()
        
        t_file = next((os.path.join(temp_dir, n) for n in os.listdir(temp_dir) if n.endswith(".torrent")), None)
        if not t_file: return await status_msg.edit_text("❌ Torrent metadata ලබාගැනීම අසාර්ථකයි.")
        
        global aria2_api
        meta_dl = aria2_api.add_torrent(t_file, options={"pause": "true"})
        files, t_name = meta_dl.files, meta_dl.name
        
        tree_html = build_file_tree(files, True)
        tree_txt = build_file_tree(files, False)
        
        caption = (f"✅ <b>ගොනුව හඳුනාගත්තා!</b>\n\n👉 <b>මෙම පණිවිඩයට Reply කරමින්</b> අවශ්‍ය file අංක දෙන්න.\n"
                   f"උදා: <code>1,3,5-7</code> (සියල්ලට: <code>all</code>)")
        
        if len(tree_html) > 3500:
            txt_path = os.path.join(temp_dir, "file_list.txt")
            with open(txt_path, "w") as f: f.write(tree_txt)
            prompt = await message.reply_document(document=txt_path, caption=caption, parse_mode=enums.ParseMode.HTML)
        else:
            prompt = await message.reply_text(f"<b>Files:</b>\n{tree_html}\n\n{caption}", parse_mode=enums.ParseMode.HTML)
            
        task_id = str(prompt.id)
        current_tasks[task_id] = {"torrent": t_file, "files": files, "t_name": t_name, "temp": temp_dir}
        await status_msg.delete()
    except Exception as e:
        await status_msg.edit_text(f"❌ Error: {e}")

# --- REPLY COMMANDS & FILE SELECTION ---
async def reply_handler(client, message):
    if not message.reply_to_message: return
    r_id = str(message.reply_to_message.id)
    text = message.text.strip().lower()
    
    # 1. Torrent File Selection
    if r_id in current_tasks:
        task_data = current_tasks.pop(r_id)
        selected = parse_selection(text, len(task_data["files"]))
        if not selected: return await message.reply("❌ Invalid selection.")
        
        task_data["selected"] = selected
        new_task_id = str(message.id)
        current_tasks[new_task_id] = task_data
        
        await message.reply_text(
            f"✅ Files තෝරාගත්තා! කරුණාකර පහත Encoding Panel එකෙන් අවශ්‍ය ක්‍රියාව තෝරන්න:",
            reply_markup=get_panel_markup(new_task_id),
            parse_mode=enums.ParseMode.HTML,
            quote=True
        )
        return
        
    # 2. Direct Reply Commands on Media (/480, /removesub, etc.)
    if message.reply_to_message.document or message.reply_to_message.video:
        if text.startswith("/"):
            action = text.replace("/", "")
            await handle_panel_action(client, message.reply_to_message.chat.id, str(message.id), action, message.reply_to_message, is_telegram_file=True)

# --- PANEL CALLBACKS ---
async def cb_handler(client, cb):
    data = cb.data
    if data.startswith("panel_"):
        parts = data.split("_", 2)
        action, task_id = parts[1], parts[2]
        
        if action == "cancel":
            return await cb.message.edit_text("🚫 <b>ක්‍රියාවලිය අවලංගු කරන ලදී.</b>", parse_mode=enums.ParseMode.HTML)
            
        await cb.message.edit_reply_markup(None)
        await handle_panel_action(client, cb.message.chat.id, task_id, action, cb.message.reply_to_message, is_telegram_file=(task_id not in current_tasks))

async def handle_panel_action(client, chat_id, task_id, action, media_msg=None, is_telegram_file=False):
    ui_msg = await client.send_message(chat_id, "⏳ <b>Processing ආරම්භ කරමින්...</b>", parse_mode=enums.ParseMode.HTML)
    cancel_flags[task_id] = False
    
    if is_telegram_file and media_msg:
        # Download from TG
        dl_dir = f"/content/dl_{task_id}"
        os.makedirs(dl_dir, exist_ok=True)
        file_name = getattr(media_msg.document or media_msg.video, "file_name", "downloaded.mkv")
        path = os.path.join(dl_dir, file_name)
        await ui_msg.edit_text("📥 <b>Telegram වෙතින් භාගත කරමින් පවතී...</b>", parse_mode=enums.ParseMode.HTML)
        path = await client.download_media(media_msg, file_name=path)
        await process_media_file(client, chat_id, path, action, {}, 1, ui_msg, task_id)
        shutil.rmtree(dl_dir, ignore_errors=True)
    elif not is_telegram_file and task_id in current_tasks:
        # Download from Torrent
        t_data = current_tasks.pop(task_id)
        dl_dir = f"/content/dl_{task_id}"
        os.makedirs(dl_dir, exist_ok=True)
        global aria2_api
        dl = aria2_api.add_torrent(t_data["torrent"], options={"dir": dl_dir, "select-file": ",".join(map(str, t_data["selected"]))})
        
        while dl.status not in ["complete", "error", "removed"]:
            await asyncio.sleep(2)
            dl.update()
            if cancel_flags.get(task_id):
                aria2_api.remove([dl], force=True, files=False)
                break
            if dl.total_length > 0:
                await update_ui(ui_msg, "📥 Torrent Download", t_data["t_name"], dl.completed_length, dl.total_length, time.time(), [0], task_id)
                
        if cancel_flags.get(task_id): return await ui_msg.edit_text("🚫 Cancelled.")
        
        for f in dl.files:
            if getattr(f, "selected", False) and os.path.exists(str(f.path)):
                await process_media_file(client, chat_id, str(f.path), action, {}, f.index, ui_msg, task_id)
        shutil.rmtree(dl_dir, ignore_errors=True)

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
            
            # Register Handlers Programmatically
            app.add_handler(MessageHandler(start_cmd, filters.command("start")))
            app.add_handler(MessageHandler(help_cmd, filters.command("help")))
            app.add_handler(MessageHandler(handle_leech, filters.command("leech")))
            app.add_handler(MessageHandler(handle_telegram_file, (filters.document | filters.video) & filters.private))
            app.add_handler(MessageHandler(reply_handler, filters.reply & filters.text))
            app.add_handler(CallbackQueryHandler(cb_handler))
            
            asyncio.create_task(heartbeat_loop(ws))
            await app.start()
            logger.info("✅ Colab Worker is ONLINE!")
            await ws.wait_closed()
            
    except Exception as e:
        logger.error(f"Failed to connect: {e}")
    finally:
        if app and app.is_connected: await app.stop()

async def start_cmd(client, message):
    await message.reply("⚡ **Colab Worker is Online & Ready!**\nUse `/leech <magnet_link>` to start downloading or `/help` to see commands.")

if __name__ == "__main__":
    asyncio.run(main())
