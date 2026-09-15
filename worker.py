import asyncio
import os
import websockets
from wzgram import Client, filters
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

async def download_torrent(magnet_link: str, download_dir: str = "./downloads", status_callback=None):
    os.makedirs(download_dir, exist_ok=True)
    # Using aria2c for fast downloading
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
    
    # Read output line by line for live status
    while True:
        line = await process.stdout.readline()
        if not line:
            break
        line_str = line.decode('utf-8', errors='ignore').strip()
        
        # aria2c progress line looks like: [#xxxxxx 10MiB/100MiB(10%) CN:1 SD:1 DL:1MiB ETA:1m]
        if line_str.startswith("[#") and status_callback:
            # We don't await the callback to avoid blocking the stdout reader
            asyncio.create_task(status_callback(line_str))
            
    await process.wait()
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
    
    async def progress_update(progress_str):
        nonlocal last_update_time
        current_time = time.time()
        # Update message at most every 4 seconds to avoid Telegram flood limits
        if current_time - last_update_time > 4:
            try:
                await status_msg.edit_text(f"🚀 **Downloading...**\n\n`{progress_str}`")
                last_update_time = current_time
            except Exception:
                pass
    
    downloaded_file = await download_torrent(magnet_link, status_callback=progress_update)
    
    if not downloaded_file:
        await status_msg.edit_text("❌ **Download Failed!** Please check the magnet link.")
        return
        
    await status_msg.edit_text("📤 **Download complete! Uploading to Telegram using WZGram fast uploader...**")
    
    try:
        uploader = user_app if user_app else client
        # If using premium user_app, ensure it is connected
        if user_app and not user_app.is_connected:
            await user_app.start()
            
        await uploader.send_document(
            chat_id=message.chat.id,
            document=downloaded_file,
            caption="Here is your file, processed by Colab! ⚡️"
        )
        await status_msg.edit_text("✅ **Successfully uploaded!**")
    except Exception as e:
        await status_msg.edit_text(f"❌ **Upload Failed!**\nError: {e}")
    finally:
        # Cleanup
        if os.path.exists(downloaded_file):
            os.remove(downloaded_file)

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
