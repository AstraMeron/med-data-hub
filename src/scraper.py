import os
import json
import asyncio
import logging
from telethon import TelegramClient
from dotenv import load_dotenv
from datetime import datetime

# 1. Setup Logging
os.makedirs('logs', exist_ok=True)
logging.basicConfig(
    filename='logs/scraping.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

load_dotenv()
api_id = os.getenv('TG_API_ID')
api_hash = os.getenv('TG_API_HASH')

channels = ['CheMed123', 'lobelia4cosmetics', 'tikvahpharma', 'DoctorsET']

async def scrape_channel(client, channel_username):
    logger.info(f"Starting scrape for {channel_username}...") # Log start
    
    today = datetime.now().strftime('%Y-%m-%d')
    output_dir = f"data/raw/telegram_messages/{today}"
    os.makedirs(output_dir, exist_ok=True)
    
    image_dir = f"data/raw/images/{channel_username}"
    os.makedirs(image_dir, exist_ok=True)

    messages_data = []

    try:
        async for message in client.iter_messages(channel_username, limit=100):
            msg_info = {
                'message_id': message.id,
                'channel_name': channel_username,
                'message_date': str(message.date),
                'message_text': message.message,
                'views': message.views,
                'forwards': message.forwards,
            }

            if message.photo:
                file_path = f"{image_dir}/{message.id}.jpg"
                await client.download_media(message.photo, file=file_path)
                msg_info['image_path'] = file_path

            messages_data.append(msg_info)
        
        # Save to JSON
        with open(f"{output_dir}/{channel_username}.json", "w", encoding='utf-8') as f:
            json.dump(messages_data, f, indent=4, ensure_ascii=False)
        
        logger.info(f"Successfully scraped {len(messages_data)} messages from {channel_username}")

    except Exception as e:
        logger.error(f"Error scraping {channel_username}: {e}") # Log errors

async def main():
    client = TelegramClient('scraping_session', api_id, api_hash)
    await client.start()
    await asyncio.gather(*(scrape_channel(client, c) for c in channels))
    logger.info("Total scraping task finished.")

if __name__ == "__main__":
    asyncio.run(main())