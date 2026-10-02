import asyncio
import os
import websockets
import json
from wzgram import Client, filters, enums
from wzgram.types import InlineKeyboardMarkup, InlineKeyboardButton
from dotenv import load_dotenv
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

API_ID = int(os.getenv("API_ID", 38017568))
API_HASH = os.getenv("API_HASH", "edce8874495cfda158be92a333fa4823")
BOT_TOKEN = os.getenv("BOT_TOKEN", "8605413664:AAHvjzRz-Xf_ZvTZKOXi-Eq6ixTKGfEqD6o")
PREMIUM_SESSION = os.getenv("PREMIUM_SESSION", "WZ_AwUCRBogABS8Wb1qowLpUFBAAntHiLOaqiMZJMfHkgTu0vKE6oWPk-XUZ5W0urGzkhtY1HzKtT831JPLuLng7XXUvzdspOvtjVIkv2sgDNefXfsf57fsCOvwatiwdbvE2wKuwkQPDH2Rt8JeJD107wFWQrxpAOEEv--tRLgUHEkqR3Lm0nnVAUZwgYwUqY0lZ--o4Hl6piaDq3oEAhpyJob7ciYtgsvxh8r5qI6TNCdF1APmMySP_VHaae4OsKEGlWDs-BDLMZGBgunEQVojP2KWroO8JxtTNANrxJTg9_BDlN9XSm2KZiC7m-WGLLUxojJuxK505JOz_RqZ0D5BjCFN_QGtkCYAAAAAdj-wVAABuzkxLjEwOC41Ni4xODEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABu5C90")
PORT = int(os.getenv("PORT", 8080))

# WZGram Bot Instance for Railway
app = None
bot_running = False

pending_connections = {}

async def start_cmd(client, message):
    colab_code = (
        "**⚡️ Master Bot is Online!**\n\n"
        "Choose your Colab Setup below. **Tap the code block to copy it.**\n\n"
        "🟢 **Option 1: NORMAL SETUP (No 4K)**\n"
        "!apt-get update && apt-get install -y aria2 ffmpeg mkvtoolnix\n"
        "!pip install -U wzgram[fast] websockets python-dotenv aria2p nest_asyncio ffmpeg-python\n"
        "!wget -qO worker.py https://raw.githubusercontent.com/hatpeti/colbconnect/main/worker.py\n"
        "!python worker.py\n\n"
        
        "🔴 **Option 2: 4K (Real-ESRGAN 6B) SETUP**\n"
        "!apt-get update && apt-get install -y aria2 ffmpeg mkvtoolnix\n"
        "!pip install -U wzgram[fast] websockets python-dotenv aria2p nest_asyncio ffmpeg-python\n"
        "!git clone https://github.com/xinntao/BasicSR.git /content/BasicSR\n"
        "%cd /content/BasicSR\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'1.4.2\\'/g" setup.py\n"
        "!sed -i "s/functional_tensor/functional/g" basicsr/data/degradations.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "%cd /content\n"
        "!git clone https://github.com/xinntao/Real-ESRGAN.git /content/Real-ESRGAN\n"
        "%cd /content/Real-ESRGAN\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'0.2.5.0\\'/g" setup.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "!pip install facexlib gfpgan\n"
        "!wget -q https://github.com/xinntao/Real-ESRGAN/releases/download/v0.2.2.4/RealESRGAN_x4plus_anime_6B.pth -P weights/\n"
        "%cd /content\n"
        "!wget -qO worker.py https://raw.githubusercontent.com/hatpeti/colbconnect/main/worker.py\n"
        "!python worker.py\n\n"
        
        "🟠 **Option 3: 4K (Real-ESRGAN v3 Fast) SETUP**\n"
        "!apt-get update && apt-get install -y aria2 ffmpeg mkvtoolnix\n"
        "!pip install -U wzgram[fast] websockets python-dotenv aria2p nest_asyncio ffmpeg-python\n"
        "!git clone https://github.com/xinntao/BasicSR.git /content/BasicSR\n"
        "%cd /content/BasicSR\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'1.4.2\\'/g" setup.py\n"
        "!sed -i "s/functional_tensor/functional/g" basicsr/data/degradations.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "%cd /content\n"
        "!git clone https://github.com/xinntao/Real-ESRGAN.git /content/Real-ESRGAN\n"
        "%cd /content/Real-ESRGAN\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'0.2.5.0\\'/g" setup.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "!pip install facexlib gfpgan\n"
        "!wget -q https://github.com/xinntao/Real-ESRGAN/releases/download/v0.2.5.0/realesr-animevideov3.pth -P weights/\n"
        "%cd /content\n"
        "!wget -qO worker.py https://raw.githubusercontent.com/hatpeti/colbconnect/main/worker.py\n"
        "!python worker.py\n\n"

        "🔵 **Option 4: 4K (Anime4K & Real-CUGAN) SETUP**\n"
        "!apt-get update && apt-get install -y aria2 ffmpeg mkvtoolnix\n"
        "!pip install -U wzgram[fast] websockets python-dotenv aria2p nest_asyncio ffmpeg-python\n"
        "!wget https://github.com/bilibili/ailab/releases/download/real-cugan/realcugan-ncnn-vulkan-20220728-ubuntu.zip\n"
        "!unzip -o realcugan-ncnn-vulkan-20220728-ubuntu.zip -d /content/realcugan\n"
        "!chmod +x /content/realcugan/realcugan-ncnn-vulkan\n"
        "!wget https://github.com/TianZerL/Anime4KCPP/releases/download/v2.5.0/Anime4KCPP_CLI-linux-x64.zip\n"
        "!unzip -o Anime4KCPP_CLI-linux-x64.zip -d /content/anime4k\n"
        "!chmod +x /content/anime4k/Anime4KCPP_CLI\n"
        "!wget -qO worker.py https://raw.githubusercontent.com/hatpeti/colbconnect/main/worker.py\n"
        "!python worker.py\n\n"
        
        "🟣 **Option 5: ALL IN ONE 4K MODELS SETUP**\n"
        "!apt-get update && apt-get install -y aria2 ffmpeg mkvtoolnix\n"
        "!pip install -U wzgram[fast] websockets python-dotenv aria2p nest_asyncio ffmpeg-python\n"
        "!git clone https://github.com/xinntao/BasicSR.git /content/BasicSR\n"
        "%cd /content/BasicSR\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'1.4.2\\'/g" setup.py\n"
        "!sed -i "s/functional_tensor/functional/g" basicsr/data/degradations.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "%cd /content\n"
        "!git clone https://github.com/xinntao/Real-ESRGAN.git /content/Real-ESRGAN\n"
        "%cd /content/Real-ESRGAN\n"
        "!sed -i "s/return locals()\\[\\'__version__\\'\\]/return \\'0.2.5.0\\'/g" setup.py\n"
        "!pip install -r requirements.txt && python setup.py develop\n"
        "!pip install facexlib gfpgan\n"
        "!wget -q https://github.com/xinntao/Real-ESRGAN/releases/download/v0.2.2.4/RealESRGAN_x4plus_anime_6B.pth -P weights/\n"
        "!wget -q https://github.com/xinntao/Real-ESRGAN/releases/download/v0.2.5.0/realesr-animevideov3.pth -P weights/\n"
        "%cd /content\n"
        "!wget https://github.com/bilibili/ailab/releases/download/real-cugan/realcugan-ncnn-vulkan-20220728-ubuntu.zip\n"
        "!unzip -o realcugan-ncnn-vulkan-20220728-ubuntu.zip -d /content/realcugan\n"
        "!chmod +x /content/realcugan/realcugan-ncnn-vulkan\n"
        "!wget https://github.com/TianZerL/Anime4KCPP/releases/download/v2.5.0/Anime4KCPP_CLI-linux-x64.zip\n"
        "!unzip -o Anime4KCPP_CLI-linux-x64.zip -d /content/anime4k\n"
        "!chmod +x /content/anime4k/Anime4KCPP_CLI\n"
        "!wget -qO worker.py https://raw.githubusercontent.com/hatpeti/colbconnect/main/worker.py\n"
        "!python worker.py\n\n"

        "Once Colab gives you a key, use /colab here to authorize it."
    )
    await message.reply(colab_code, parse_mode=enums.ParseMode.MARKDOWN)

async def colab_cmd(client, message):
    if not pending_connections:
        await message.reply("ℹ️ **No pending Colab connections.**\nStart your Colab notebook first, then try again.")
        return
        
    buttons = []
    for key in pending_connections.keys():
        buttons.append([InlineKeyboardButton(f"✅ Authorize {key}", callback_data=f"auth_{key}", style=enums.ButtonStyle.SUCCESS)])
        
    markup = InlineKeyboardMarkup(buttons)
    await message.reply("🔗 **Pending Colab Connections:**\nSelect an instance to authorize it.", reply_markup=markup)

async def handle_auth(client, callback_query):
    key = callback_query.matches[0].group(1)
    if key in pending_connections:
        ws_info = pending_connections[key]
        ws = ws_info["ws"]
        
        payload = {
            "status": "AUTHORIZED",
            "API_ID": API_ID,
            "API_HASH": API_HASH,
            "BOT_TOKEN": BOT_TOKEN,
            "PREMIUM_SESSION": PREMIUM_SESSION
        }
        
        try:
            await ws.send(json.dumps(payload))
            ws_info["event"].set()
            ws_info["auth_chat_id"] = callback_query.message.chat.id
            del pending_connections[key]
            
            await callback_query.edit_message_text(f"✅ **Colab Worker {key} Authorized successfully!**\nHanding over control to Colab...")
            asyncio.create_task(stop_telegram_bot())
        except Exception as e:
            await callback_query.answer(f"Failed to send credentials: {e}", show_alert=True)
    else:
        await callback_query.answer("❌ Invalid or expired connection key.", show_alert=True)

async def leech_cmd(client, message):
    await message.reply("⚠️ **Colab Worker is currently Offline!**\n\nPlease start your Google Colab notebook to use the `/leech` command. Once Colab connects, this bot will hand over control automatically.")

def register_handlers(bot):
    bot.on_message(filters.command("start"))(start_cmd)
    bot.on_message(filters.command("colab"))(colab_cmd)
    bot.on_message(filters.command("leech"))(leech_cmd)
    bot.on_callback_query(filters.regex(r"^auth_(\d+)$"))(handle_auth)

async def start_telegram_bot():
    global app, bot_running
    if not bot_running:
        try:
            logger.info("Starting Telegram Bot...")
            app = Client("master_bot_v2", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
            register_handlers(app)
            await app.start()
            bot_running = True
        except Exception as e:
            logger.error(f"Error starting bot: {e}")

async def stop_telegram_bot():
    global bot_running
    if bot_running:
        try:
            logger.info("Stopping Telegram Bot (Handing over to Colab)...")
            await app.stop()
            bot_running = False
        except Exception as e:
            logger.error(f"Error stopping bot: {e}")

async def handle_ws(websocket):
    logger.info("New WebSocket connection attempt...")
    key = None
    try:
        # Colab worker sends its generated key
        key = await websocket.recv()
        logger.info(f"Received connection key: {key}")
        
        event = asyncio.Event()
        ws_info = {"ws": websocket, "event": event}
        pending_connections[key] = ws_info
        
        # Wait until authorized
        await event.wait()
        
        logger.info(f"Colab Worker {key} Connected Successfully!")

        # Keep connection open until Colab disconnects
        async for message in websocket:
            if message == "ping":
                await websocket.send("pong")
                
    except websockets.exceptions.ConnectionClosed:
        logger.info("WebSocket connection closed abruptly.")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        if key in pending_connections:
            del pending_connections[key]
        logger.info("Colab Worker Disconnected! Restarting local bot...")
        await start_telegram_bot()
        
        # Notify the user if it was an authorized connection
        auth_chat_id = ws_info.get("auth_chat_id") if 'ws_info' in locals() else None
        if auth_chat_id:
            try:
                await app.send_message(auth_chat_id, "⚠️ **Colab Worker Disconnected!**\n\nThe master bot has resumed control. Please restart your Colab notebook to continue.")
            except Exception as e:
                logger.error(f"Failed to send disconnect notification: {e}")

async def main():
    if API_ID == 0 or not BOT_TOKEN:
        logger.error("Please set API_ID, API_HASH, and BOT_TOKEN in the .env file!")
        return

    # Start the bot initially
    await start_telegram_bot()
    
    # Start WebSocket Server
    logger.info(f"Starting WebSocket server on port {PORT}")
    async with websockets.serve(handle_ws, "0.0.0.0", PORT):
        # Run forever
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
