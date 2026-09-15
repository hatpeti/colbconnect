import asyncio
import os
import websockets
import json
import random
from wzgram import Client, filters, enums
from wzgram.handlers import MessageHandler, CallbackQueryHandler
from wzgram.types import InlineKeyboardMarkup, InlineKeyboardButton
import logging
import time
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MASTER_WS_URL = "wss://leech-production-214b.up.railway.app"

app = None
user_app = None

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

def format_upload_progress(current, total, start_time, user_name="User"):
    percent = round(current * 100 / total, 1) if total else 0
    filled = int(percent // 10)
    bar = "■" * filled + "□" * (10 - filled)
    
    elapsed = time.time() - start_time
    speed = current / elapsed if elapsed > 0 else 0
    speed_str = f"{speed / 1024 / 1024:.2f} MB/s"
    
    if speed > 0:
        eta_seconds = (total - current) / speed
        eta = f"{int(eta_seconds // 60)}m {int(eta_seconds % 60)}s"
    else:
        eta = "Unknown"
        
    downloaded_str = f"{current / 1024 / 1024:.2f} MB"
    total_str = f"{total / 1024 / 1024:.2f} MB"
    
    text = (
        f"Task By 👤 {user_name}\n"
        f"├ [{bar}] {percent}%\n"
        f"├ Processed → {downloaded_str} of {total_str}\n"
        f"├ Status → Uploading to Telegram\n"
        f"├ Speed → {speed_str}\n"
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

async def start_cmd(client, message):
    await message.reply("⚡ **Colab Worker is Online & Ready!**\nUse `/leech <magnet_link>` to start downloading.")

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
        
    await status_msg.edit_text("📤 **Download complete! Uploading to @animedubsinhla channel...**")
    
    upload_start_time = time.time()
    last_upload_update = 0
    
    async def upload_progress(current, total):
        nonlocal last_upload_update
        current_time = time.time()
        if current_time - last_upload_update > 4:
            formatted_text = format_upload_progress(current, total, upload_start_time, user_name)
            try:
                await status_msg.edit_text(f"`{formatted_text}`", reply_markup=markup)
                last_upload_update = current_time
            except Exception:
                pass

    try:
        uploader = user_app if user_app else client
        if user_app and not user_app.is_connected:
            await user_app.start()
            
        await uploader.send_document(
            chat_id="@animedubsinhla",
            document=downloaded_file,
            caption="Here is your file, processed by Colab! ⚡️",
            progress=upload_progress
        )
        await status_msg.edit_text("✅ **Successfully uploaded to @animedubsinhla!**", reply_markup=InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Done", callback_data="done", style=enums.ButtonStyle.SUCCESS)
        ]]))
    except Exception as e:
        await status_msg.edit_text(f"❌ **Upload Failed!**\nError: {e}")
    finally:
        if os.path.exists(downloaded_file):
            os.remove(downloaded_file)

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
    global app, user_app
    
    # Generate pairing key
    key = str(random.randint(100000, 999999))
    print("="*60)
    print(f"YOUR CONNECTION KEY IS: {key}")
    print("Send `/colab` to your Telegram Master Bot and authorize this key.")
    print("="*60)
    
    logger.info(f"Connecting to Railway Master at {MASTER_WS_URL}...")
    try:
        async with websockets.connect(MASTER_WS_URL) as websocket:
            # Send the key for authentication
            await websocket.send(key)
            logger.info("Key sent! Waiting for authorization from Master Bot...")
            
            # This will block until the owner clicks the Authorize button on Telegram
            response = await websocket.recv()
            
            try:
                data = json.loads(response)
            except json.JSONDecodeError:
                logger.error("Failed to parse JSON response from Master!")
                return
                
            if data.get("status") != "AUTHORIZED":
                logger.error("Authentication failed with Master Server!")
                return
                
            logger.info("✅ Connected to Master Server! Receiving credentials...")
            
            api_id = data.get("API_ID")
            api_hash = data.get("API_HASH")
            bot_token = data.get("BOT_TOKEN")
            premium_session = data.get("PREMIUM_SESSION")
            
            if not api_id or not bot_token:
                logger.error("Master did not provide valid credentials!")
                return
                
            app = Client("colab_worker", api_id=api_id, api_hash=api_hash, bot_token=bot_token)
            
            if premium_session:
                user_app = Client("premium_uploader", api_id=api_id, api_hash=api_hash, session_string=premium_session)
                
            # Add handlers programmatically
            app.add_handler(MessageHandler(start_cmd, filters.command("start")))
            app.add_handler(MessageHandler(handle_leech, filters.command("leech")))
            app.add_handler(CallbackQueryHandler(cancel_download, filters.regex(r"^cancel_(\d+)$")))
            
            asyncio.create_task(heartbeat_loop(websocket))
            
            # Start the Telegram Bot on Colab
            await app.start()
            logger.info("Bot is now running on Colab with received credentials!")
            
            # Keep running until websocket disconnects
            await websocket.wait_closed()
            
    except Exception as e:
        logger.error(f"Failed to connect to Master: {e}")
    finally:
        if app and app.is_connected:
            await app.stop()
        if user_app and user_app.is_connected:
            await user_app.stop()

if __name__ == "__main__":
    asyncio.run(main())
