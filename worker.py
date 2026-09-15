import asyncio
import os
import websockets
from wzgram import Client, filters, enums
from wzgram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
PREMIUM_SESSION = os.environ.get("PREMIUM_SESSION", "") # Optional for 4GB uploads
SECRET_TOKEN = os.environ.get("WS_SECRET", "supersecret")
MASTER_WS_URL = os.environ.get("MASTER_WS_URL", "ws://localhost:8080")

# Use Premium session if provided, otherwise fallback to bot token for uploads
app = Client("colab_worker", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
user_app = None

if PREMIUM_SESSION:
    user_app = Client("premium_uploader", api_id=API_ID, api_hash=API_HASH, session_string=PREMIUM_SESSION)

import time
import re

def format_progress(line_str, user_name="User"):
    match = re.search(r' ([\d\.]+.*?)/([\d\.]+.*?)\(([\d\.]+)%\).*?DL:([\d\.]+.*?)(?: ETA:(.*?))?\]', line_str)
    if not match:
        return f"📥 **Downloading...**\n`{line_str}`"
        
    downloaded, total, percent_str, speed = match.group(1), match.group(2), match.group(3), match.group(4)
    eta = match.group(5) if match.group(5) else "Unknown"
    
    try:
        percent = float(percent_str)
    except ValueError:
        percent = 0.0
        
    filled = int(percent // 10)
    bar = "■" * filled + "□" * (10 - filled)
    
    text = (
        f"Task By 👤 {user_name}\n"
        f"├ [{bar}] {percent}%\n"
        f"├ Processed → {downloaded} of {total}\n"
        f"├ Status → Downloading (Aria2c)\n"
        f"├ Speed → {speed}/s\n"
        f"└ Time → {eta}"
    )
    return text

active_downloads = {}

async def download_torrent(magnet_link: str, download_dir: str = "./downloads", status_callback=None, task_id=None):
    os.makedirs(download_dir, exist_ok=True)
    cmd = [
        "aria2c",
        "--seed-time=0",
        "--dir", download_dir,
        "--summary-interval=3",
        magnet_link
    ]
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE
    )
    
    if task_id:
        active_downloads[task_id] = process
        
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        line_str = line.decode('utf-8', errors='ignore').strip()
        if line_str.startswith("[#") and status_callback:
            asyncio.create_task(status_callback(line_str))
            
    await process.wait()
    if task_id in active_downloads:
        del active_downloads[task_id]
        
    if process.returncode == 0:
        logger.info("Download completed!")
        files = os.listdir(download_dir)
        if files:
            return os.path.join(download_dir, files[0])
    return None

@app.on_message(filters.command("leech"))
async def handle_leech(client, message):
    if len(message.command) < 2:
        await message.reply("Please provide a magnet link! Example: `/leech magnet:?...`")
        return

    magnet_link = message.command[1]
    status_msg = await message.reply("🚀 **Colab Worker is starting download...**\n`Connecting to peers...`")
    
    last_update_time = 0
    user_name = message.from_user.first_name if message.from_user else "User"
    task_id = str(message.id)
    
    markup = InlineKeyboardMarkup([[
        InlineKeyboardButton("🛑 Cancel", callback_data=f"cancel_{task_id}", style=enums.ButtonStyle.DANGER),
        InlineKeyboardButton("🌐 Status", callback_data="status", style=enums.ButtonStyle.PRIMARY)
    ]])
    
    async def progress_update(progress_str):
        nonlocal last_update_time
        current_time = time.time()
        if current_time - last_update_time > 4:
            formatted_text = format_progress(progress_str, user_name)
            try:
                await status_msg.edit_text(f"`{formatted_text}`", reply_markup=markup)
                last_update_time = current_time
            except Exception:
                pass
    
    downloaded_file = await download_torrent(magnet_link, status_callback=progress_update, task_id=task_id)
    
    if not downloaded_file:
        await status_msg.edit_text("❌ **Download Failed or Cancelled!**")
        return
        
    await status_msg.edit_text("📤 **Download complete! Uploading to Telegram using WZGram fast uploader...**")
    
    try:
        uploader = user_app if user_app else client
        if user_app and not user_app.is_connected:
            await user_app.start()
            
        await uploader.send_document(
            chat_id=message.chat.id,
            document=downloaded_file,
            caption="Here is your file, processed by Colab! ⚡️"
        )
        await status_msg.edit_text("✅ **Successfully uploaded!**", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Done", callback_data="done", style=enums.ButtonStyle.SUCCESS)
        ]]))
    except Exception as e:
        await status_msg.edit_text(f"❌ **Upload Failed!**\nError: {e}")
    finally:
        if os.path.exists(downloaded_file):
            os.remove(downloaded_file)

@app.on_callback_query(filters.regex(r"^cancel_(\d+)$"))
async def cancel_download(client, callback_query):
    task_id = callback_query.matches[0].group(1)
    if task_id in active_downloads:
        process = active_downloads[task_id]
        process.terminate()
        del active_downloads[task_id]
        await callback_query.answer("Download cancelled!", show_alert=True)
    else:
        await callback_query.answer("Download not found or already finished.", show_alert=True)

async def heartbeat_loop(websocket):
    try:
        while True:
            await asyncio.sleep(10)
            await websocket.send("ping")
            await websocket.recv()
    except websockets.exceptions.ConnectionClosed:
        logger.warning("Master server disconnected!")
        
async def main():
    if API_ID == 0 or not BOT_TOKEN:
        logger.error("Please configure API_ID and BOT_TOKEN!")
        return

    logger.info(f"Connecting to Railway Master at {MASTER_WS_URL}...")
    try:
        async with websockets.connect(MASTER_WS_URL) as websocket:
            # Authenticate
            await websocket.send(SECRET_TOKEN)
            response = await websocket.recv()
            
            if response != "AUTHORIZED":
                logger.error("Authentication failed with Master Server!")
                return
                
            logger.info("Connected to Master Server! Taking over Telegram Bot...")
            
            asyncio.create_task(heartbeat_loop(websocket))
            
            # Start the Telegram Bot on Colab
            await app.start()
            logger.info("Bot is now running on Colab!")
            
            # Keep running until websocket disconnects
            await websocket.wait_closed()
            
    except Exception as e:
        logger.error(f"Failed to connect to Master: {e}")
    finally:
        if app.is_connected:
            await app.stop()
        if user_app and user_app.is_connected:
            await user_app.stop()

if __name__ == "__main__":
    asyncio.run(main())
