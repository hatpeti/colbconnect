# Colab Worker Setup

To run this worker on Google Colab, you need to install the dependencies in a notebook cell first.

1. Open a new Google Colab notebook.
2. Run this cell to install the required tools:

```bash
!apt-get install -y aria2
!pip install -U wzgram[fast] websockets python-dotenv
```

3. Create a `.env` file in the Colab environment (or set environment variables):
```env
API_ID=your_api_id
API_HASH=your_api_hash
BOT_TOKEN=your_bot_token
PREMIUM_SESSION=your_premium_session_string
WS_SECRET=supersecret
MASTER_WS_URL=ws://YOUR_RAILWAY_APP_URL:8080
```

4. Run the worker script:
```bash
!python worker.py
```
