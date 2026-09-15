# Colab Worker Setup

To run this worker on Google Colab, you need to install the dependencies in a notebook cell first.

1. Open a new Google Colab notebook.
2. Run this cell to install the required tools:

```bash
!wget -qO- https://github.com/P3TERX/Aria2-Pro-Core/releases/download/1.37.0_2023.08.17/aria2-1.37.0-static-linux-amd64.tar.gz | tar -xz && mv aria2c /usr/local/bin/
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
