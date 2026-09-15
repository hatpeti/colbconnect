import asyncio
from wzgram import Client

# Using the API credentials provided
API_ID = 38017568
API_HASH = "edce8874495cfda158be92a333fa4823"

async def main():
    print("\n--- Telegram Session String Generator ---")
    print("Please follow the prompts below to login to your Telegram account.\n")
    
    # Start the client. It will automatically ask for phone number and OTP code.
    async with Client("temp_session", api_id=API_ID, api_hash=API_HASH) as app:
        session_string = await app.export_session_string()
        print("\n\n✅ SUCCESS! Copy the text below:\n")
        print("---------------------------------------------------------")
        print(session_string)
        print("---------------------------------------------------------\n")
        print("Paste this string into the PREMIUM_SESSION variable in your Colab worker.")

if __name__ == "__main__":
    asyncio.run(main())
