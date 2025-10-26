# A Production-Ready Python Script for Automated, Incremental Telegram Channel Scraping.
# Designed for continuous operation on a server using the 'schedule' library.
#
# NEW DEPENDENCIES REQUIRED:
# pip install telethon pandas schedule

from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
import csv
import re
import asyncio 
import os 
import time
import schedule 
import pandas as pd 

# --- CONFIGURATION (User Data Integrated) ---
API_ID = 23618024  
API_HASH = 'd9a483f0afb5f2583c937d87252747b4' 
PHONE_NUMBER = '+251960496143'

# --- AUTOMATION & LIMITS ---
SCHEDULE_INTERVAL_MINUTES = 60
LIMIT_MESSAGES_PER_CHANNEL = 200

CHANNELS = [
    '@beracomputer', 
    '@sami_brand12',  
    '@MoonLaptops'
]
OUTPUT_FILE_RAW = 'telegram_posts_raw.csv'
OUTPUT_FILE_CLEAN = 'data_for_analysis.csv'

# --- CRITICAL FIX: Ensure session file is created/loaded in the current directory ---
# This ensures the scraper works correctly when launched via 'nohup' or scheduled tasks.
SESSION_NAME = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scraper_session')

# --- UTILITY FUNCTIONS ---
def is_sold_post(text):
    if not text: return True
    sold_keywords = [
        r'\bSOLD\b', r'\bSold\b', r'\bTAKEN\b', r'\bTaken\b', r'OUT OF STOCK', 
        r'ተሽጧል', r'ጨርሷል', r'No longer available', r'unavailable'
    ]
    pattern = re.compile('|'.join(sold_keywords), re.IGNORECASE)
    return bool(pattern.search(text))

def extract_simple_price(text):
    match = re.search(r'(\d[\d,]*)(?:\s*(?:ETB|Birr|ብር))?', text, re.IGNORECASE)
    if match:
        price_str = match.group(1).replace(',', '')
        try: return int(price_str)
        except ValueError: return None
    return None

def extract_contact(text):
    match = re.search(r'(\+2519|09)\s*(\d\s*){8}', text)
    if match: return re.sub(r'\s+', '', match.group(0))
    return None

def categorize_product(text):
    text = text.lower()
    laptop_keywords = ['laptop', 'notebook', 'hp', 'dell', 'lenovo', 'macbook', 'surface', 'thinkpad', 'ultrabook']
    if any(k in text for k in laptop_keywords): return 'Laptop'
    desktop_keywords = ['desktop', 'workstation', 'tower', 'pc', 'core i7', 'core i5', 'ryzen 5', 'ryzen 7', 'gaming pc']
    if any(k in text for k in desktop_keywords): return 'Desktop/PC'
    accessory_keywords = ['ssd', 'ram', 'monitor', 'printer', 'keyboard', 'mouse', 'bag', 'adapter']
    if any(k in text for k in accessory_keywords): return 'Accessory'
    return 'Uncategorized'

def get_existing_ids(filepath):
    existing_ids = set()
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', newline='\n', encoding='utf-8') as f:
                reader = csv.reader(f)
                next(reader) 
                for row in reader:
                    if row and row[0].isdigit():
                        existing_ids.add(int(row[0]))
        except Exception as e:
            print(f"Warning: Could not read existing CSV file. Starting fresh. Error: {e}")
            return set()
    return existing_ids


# --- STAGE 1: SCRAPING AND RAW DATA COLLECTION ---

async def scrape_channel(client, channel_username, writer, existing_ids):
    print(f"[{time.strftime('%H:%M:%S')}] Starting incremental scrape for {channel_username}...")
    
    try:
        channel_entity = await client.get_entity(channel_username)
    except ValueError:
        print(f"[{time.strftime('%H:%M:%S')}] Error: Channel '{channel_username}' not found. Skipping.")
        return 0

    total_new_messages = 0
    skipped_sold_count = 0
    
    try:
        messages = await client.get_messages(channel_entity, limit=LIMIT_MESSAGES_PER_CHANNEL)
    except Exception as e:
        print(f"[{time.strftime('%H:%M:%S')}] API Error on {channel_username}: {e}")
        return 0

    for message in messages:
        if message.message and message.id:
            msg_id = message.id
            text = message.message
            
            if msg_id in existing_ids: continue
            if is_sold_post(text):
                skipped_sold_count += 1
                continue

            price = extract_simple_price(text)
            contact = extract_contact(text)
            category = categorize_product(text) 

            writer.writerow([
                msg_id, channel_username, message.date.isoformat(),
                text.replace('\n', ' ').strip(), 
                message.views if message.views else 0,
                price, contact, category
            ])
            total_new_messages += 1

    print(f"[{time.strftime('%H:%M:%S')}] Finished {channel_username}. Added {total_new_messages} NEW items (Skipped {skipped_sold_count} sold posts).")
    return total_new_messages


async def main_scraper_task():
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH) # <-- Use SESSION_NAME here
    
    print(f"\n--- SCRAPER JOB STARTED at {time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    try:
        await client.start(phone=PHONE_NUMBER)
    except Exception as e:
        print(f"CRITICAL ERROR: Failed to connect to Telegram. Check API keys: {e}")
        return

    print(f"[{time.strftime('%H:%M:%S')}] Successfully connected.")

    existing_ids = get_existing_ids(OUTPUT_FILE_RAW)
    is_new_file = not os.path.exists(OUTPUT_FILE_RAW)

    with open(OUTPUT_FILE_RAW, 'a', newline='\n', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        if is_new_file or not existing_ids: 
             writer.writerow([
                'message_id', 'source_channel', 'date', 'raw_text', 'views', 
                'simple_price_etb', 'extracted_contact', 'product_category'
            ])

        scrape_tasks = [scrape_channel(client, channel, writer, existing_ids) for channel in CHANNELS]
        results = await asyncio.gather(*scrape_tasks)
        all_scraped_count = sum(results)

    await client.disconnect()
    print(f"[{time.strftime('%H:%M:%S')}] Scraping finished. Total NEW items added: {all_scraped_count}.")
    
    clean_and_prepare_data()
    
    print(f"--- SCRAPER JOB FINISHED at {time.strftime('%Y-%m-%d %H:%M:%S')} ---")


# --- STAGE 2: DATA CLEANING AND PREPARATION (FOR FRONTEND/MODEL) ---

def clean_and_prepare_data():
    if not os.path.exists(OUTPUT_FILE_RAW):
        print(f"[{time.strftime('%H:%M:%S')}] Skipping cleaning: Raw file not found.")
        return

    print(f"[{time.strftime('%H:%M:%S')}] Starting data cleaning for analysis...")
    
    try:
        df = pd.read_csv(OUTPUT_FILE_RAW, dtype={'message_id': str})
        
        df.sort_values(by='date', ascending=False, inplace=True)
        df.drop_duplicates(subset=['message_id'], keep='first', inplace=True)
        df['date'] = pd.to_datetime(df['date'])
        df.dropna(subset=['simple_price_etb'], inplace=True)
        
        # Save to a clean JSON file for easy website reading (Websites prefer JSON over CSV)
        df.to_json(OUTPUT_FILE_CLEAN.replace('.csv', '.json'), orient='records', date_format='iso', indent=4)
        
        print(f"[{time.strftime('%H:%M:%S')}] Data cleaning complete. {len(df)} unique records saved to {OUTPUT_FILE_CLEAN.replace('.csv', '.json')}")

    except Exception as e:
        print(f"CRITICAL ERROR during data cleaning: {e}")


# --- EXECUTION AND SCHEDULING LOOP (Server-Ready) ---

def scheduled_job_wrapper():
    try:
        asyncio.run(main_scraper_task())  # Properly manage the event loop
    except RuntimeError as e:
        print(f"[{time.strftime('%H:%M:%S')}] Error: {e}")

if __name__ == '__main__':
    print("--- SERVER INITIALIZATION ---")
    print(f"Scraper will run every {SCHEDULE_INTERVAL_MINUTES} minutes.")
    
    scheduled_job_wrapper() 
    
    schedule.every(SCHEDULE_INTERVAL_MINUTES).minutes.do(scheduled_job_wrapper)
    
    while True:
        schedule.run_pending()
        time.sleep(1)
