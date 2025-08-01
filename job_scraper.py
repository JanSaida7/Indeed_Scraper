import os
import asyncio
import datetime
import hashlib
import re
import psycopg2
from psycopg2 import sql
import pandas as pd
from apify_client import ApifyClient
from dotenv import load_dotenv

# --- 1. CONFIGURATION ---
# Load credentials and configuration from the .env file
load_dotenv()

APIFY_TOKEN = os.getenv('APIFY_TOKEN')
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# --- 2. DATABASE FUNCTIONS ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✅ Successfully connected to PostgreSQL database.")
        return conn
    except Exception as e:
        print(f"❌ Error connecting to database: {e}")
        return None

def create_jobs_table(conn):
    """Creates the 'jobs' table in the database if it doesn't exist."""
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS jobs (
                job_id VARCHAR(255) PRIMARY KEY,
                job_title TEXT,
                company_name TEXT,
                location TEXT,
                post_date DATE,
                job_description TEXT,
                salary_info TEXT,
                job_url TEXT UNIQUE,
                source_website VARCHAR(50),
                extracted_at TIMESTAMP WITH TIME ZONE
            );
        """)
        conn.commit()
        print("✅ Table 'jobs' checked/created successfully.")

def parse_post_date(raw_post_date):
    """Parses relative date strings into a datetime.date object."""
    if not raw_post_date:
        return None
    
    raw_post_date_lower = str(raw_post_date).lower().strip()
    today = datetime.date.today()

    if "today" in raw_post_date_lower or "just posted" in raw_post_date_lower or "hour" in raw_post_date_lower:
        return today
    elif "yesterday" in raw_post_date_lower:
        return today - datetime.timedelta(days=1)
    elif "day" in raw_post_date_lower:
        match = re.search(r'(\d+)', raw_post_date_lower)
        if match:
            try:
                days_ago = int(match.group(1))
                return today - datetime.timedelta(days=days_ago)
            except (ValueError, TypeError):
                pass
    return None # Return None if no pattern matches

def insert_job_data(conn, job_data):
    """Inserts a single job record into the 'jobs' table, updating on conflict."""
    if not job_data.get('job_url'):
        print(f"⚠️ Warning: Job data for '{job_data.get('job_title', 'Unknown')}' has no URL; skipping.")
        return False
        
    # Generate a unique job_id from the URL if not provided
    if not job_data.get('job_id'):
        job_data['job_id'] = hashlib.md5(job_data['job_url'].encode('utf-8')).hexdigest()

    parsed_date = parse_post_date(job_data.get('post_date'))

    with conn.cursor() as cursor:
        insert_query = sql.SQL("""
            INSERT INTO jobs (job_id, job_title, company_name, location, post_date, 
                              job_description, salary_info, job_url, source_website, extracted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (job_url) DO UPDATE SET
                job_title = EXCLUDED.job_title,
                company_name = EXCLUDED.company_name,
                location = EXCLUDED.location,
                post_date = EXCLUDED.post_date,
                job_description = EXCLUDED.job_description,
                salary_info = EXCLUDED.salary_info,
                extracted_at = EXCLUDED.extracted_at;
        """)
        cursor.execute(insert_query, (
            job_data.get('job_id'),
            job_data.get('job_title'),
            job_data.get('company_name'),
            job_data.get('location'),
            parsed_date,
            job_data.get('job_description'),
            job_data.get('salary_info'),
            job_data.get('job_url'),
            "Indeed",
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        ))
        conn.commit()
    return True

# --- 3. APIFY SCRAPING LOGIC ---

async def scrape_indeed_jobs_apify(search_keyword, location, max_items=50):
    """Scrapes job data from Indeed using an Apify Actor with cleaner output."""
    print("➡️ Initializing Apify client...")
    try:
        apify_client = ApifyClient(APIFY_TOKEN)
        actor_id = 'misceres/indeed-scraper'
        print(f"▶️ Starting Apify Actor: {actor_id} for '{search_keyword}' in '{location}'...")

        run_input = {
            "position": search_keyword,
            "location": location,
            "country": "IN",
            "maxItems": max_items,
            "proxyConfiguration": {"useApifyProxy": True}
        }

        # Start the actor but don't wait for the logs
        run = apify_client.actor(actor_id).start(run_input=run_input)

        # Wait quietly for the actor to finish
        print("...Actor is running. Waiting for results (this may take a minute)...")
        actor_run = apify_client.run(run['id']).wait_for_finish()

        # Check the final status
        if actor_run['status'] != 'SUCCEEDED':
            print(f"❌ Actor run finished with status: {actor_run['status']}. Check Apify Console for details.")
            return []

        print(f"✅ Actor run SUCCEEDED. Fetching results...")
        dataset_items = apify_client.dataset(actor_run['defaultDatasetId']).list_items().items
        print(f"✅ Found {len(dataset_items)} items.")
        return dataset_items

    except Exception as e:
        print(f"❌ An unexpected error occurred during Apify scraping: {e}")
        return []

# --- 4. MAIN WORKFLOW ---

async def main():
    """Main function to orchestrate the scraping and storage workflow."""
    conn = get_db_connection()
    if not conn:
        return

    try:
        create_jobs_table(conn)
        
        scraped_jobs = await scrape_indeed_jobs_apify(
            search_keyword="Fullstack", 
            location="Hyderabad", 
            max_items=1
        )

        if not scraped_jobs:
            print("No jobs were scraped. Exiting.")
            return

        print(f"🔄 Storing {len(scraped_jobs)} jobs into PostgreSQL...")
        stored_count = 0
        for item in scraped_jobs:
            job_data = {
                "job_id": item.get('id'),
                "job_title": item.get('positionName'),
                "company_name": item.get('company'),
                "location": item.get('location'),
                "post_date": item.get('postedAt'),
                "job_description": item.get('description'),
                "salary_info": item.get('salary'),
                "job_url": item.get('url'),
            }
            if insert_job_data(conn, job_data):
                stored_count += 1
        
        print(f"✅ Successfully stored/updated {stored_count} job postings.")

    finally:
        if conn:
            conn.close()
            print("ℹ️ Database connection closed.")

# --- 5. EXECUTION BLOCK ---

if __name__ == '__main__':
    print("--- Starting Job Scraper Application ---")
    asyncio.run(main())
    print("--- Application Finished ---")