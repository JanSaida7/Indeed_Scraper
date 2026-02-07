"""
Job Scraper module for Indeed jobs.

This module handles:
- Connection to Apify for scraping.
- Database operations for storing job data.
- Data cleaning and parsing.
"""
"""
Core scraping module for Indeed Scraper.

Handles:
- Connecting to PostgreSQL database.
- Interacting with Apify API.
- Parsing and storing job data.
"""
import os
import asyncio
import datetime
import hashlib
import re
import psycopg2
from psycopg2 import sql
from apify_client import ApifyClient
from dotenv import load_dotenv

# --- 1. CONFIGURATION (Loads when imported) ---
load_dotenv()

APIFY_TOKEN = os.getenv('APIFY_TOKEN')
DB_CONFIG = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT')
}

# Access Code for Restriction
ACCESS_CODE = os.getenv('ACCESS_CODE', '12345') # Default to 12345 if not set

# --- 2. Reusable Functions ---

def get_db_connection() -> psycopg2.extensions.connection:
    """Establishes a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def create_users_table(conn):
    """Creates the 'users' table if it doesn't exist."""
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(50) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    role VARCHAR(20) DEFAULT 'normal',
                    scrape_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
            conn.commit()
            print("Users table checked/created.")
    except Exception as e:
        print(f"Error creating users table: {e}")

def create_user(username, password, role='normal'):
    """Creates a new user with a hashed password."""
    conn = get_db_connection()
    if not conn: return False
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (%s, %s, %s) RETURNING id",
                (username, password_hash, role)
            )
            user_id = cursor.fetchone()[0]
            conn.commit()
            print(f"User '{username}' created with ID {user_id} and role '{role}'.")
            return True
    except psycopg2.IntegrityError:
        print(f"User '{username}' already exists.")
        return False
    except Exception as e:
        print(f"Error creating user: {e}")
        return False
    finally:
        conn.close()

def authenticate_user(username, password):
    """Authenticates a user and returns their details if successful."""
    conn = get_db_connection()
    if not conn: return None
    
    password_hash = hashlib.sha256(password.encode()).hexdigest()
    
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT id, username, role, scrape_count FROM users WHERE username = %s AND password_hash = %s",
                (username, password_hash)
            )
            user = cursor.fetchone()
            if user:
                return {
                    "id": user[0],
                    "username": user[1],
                    "role": user[2],
                    "scrape_count": user[3]
                }
            return None
    except Exception as e:
        print(f"Authentication error: {e}")
        return None
    finally:
        conn.close()

def increment_scrape_count(user_id):
    """Increments the scrape count for a user."""
    conn = get_db_connection()
    if not conn: return
    try:
        with conn.cursor() as cursor:
            cursor.execute("UPDATE users SET scrape_count = scrape_count + 1 WHERE id = %s", (user_id,))
            conn.commit()
    except Exception as e:
        print(f"Error updating scrape count: {e}")
    finally:
        conn.close()

def get_user_limits(role):
    """Returns limit configuration based on user role."""
    if role == 'admin':
        return {"max_items": float('inf'), "max_scrapes": float('inf')}
    elif role == 'access_code':
        return {"max_items": 10, "max_scrapes": 5}
    else: # normal
        return {"max_items": 5, "max_scrapes": 3}


def parse_post_date(raw_post_date):
    if not raw_post_date: return None
    raw_post_date_lower = str(raw_post_date).lower().strip()
    today = datetime.date.today()
    if "today" in raw_post_date_lower or "just posted" in raw_post_date_lower or "hour" in raw_post_date_lower: return today
    if "yesterday" in raw_post_date_lower: return today - datetime.timedelta(days=1)
    if "day" in raw_post_date_lower:
        match = re.search(r'(\d+)', raw_post_date_lower)
        if match: return today - datetime.timedelta(days=int(match.group(1)))
    return None

def insert_job_data(conn, job_data):
    """Inserts a single job record into the 'jobs' table."""
    # Map 'url' to 'job_url' if coming from raw Apify data
    if 'url' in job_data and 'job_url' not in job_data:
        job_data['job_url'] = job_data['url']

    if not job_data.get('job_url'): 
        print(f"⚠️ Skipping item: No job_url found. Keys: {list(job_data.keys())}")
        return False
    job_data['job_id'] = job_data.get('id') or hashlib.md5(job_data['job_url'].encode('utf-8')).hexdigest()
    parsed_date = parse_post_date(job_data.get('postedAt'))
    try:
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("""
                INSERT INTO jobs (job_id, job_title, company_name, location, post_date, job_description, salary_info, job_url, source_website, extracted_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'Indeed', %s)
                ON CONFLICT (job_url) DO UPDATE SET
                    job_title = EXCLUDED.job_title, company_name = EXCLUDED.company_name, location = EXCLUDED.location,
                    post_date = EXCLUDED.post_date, job_description = EXCLUDED.job_description, salary_info = EXCLUDED.salary_info,
                    extracted_at = EXCLUDED.extracted_at;
            """), (
                job_data['job_id'], job_data.get('positionName'), job_data.get('company'), job_data.get('location'),
                parsed_date, job_data.get('description'), job_data.get('salary'), job_data.get('job_url'),
                datetime.datetime.now(datetime.timezone.utc)
            ))
            conn.commit()
    except Exception as e:
        print(f"❌ DB Insert Error: {e}")
        return False
    return True

async def scrape_and_store_jobs(keyword, location, max_items):
    """Main logic to scrape from Apify and store in the DB."""
    print("➡️ Initializing Apify client...")
    try:
        apify_client = ApifyClient(APIFY_TOKEN)
        run_input = {"position": keyword, "location": location, "country": "IN", "maxItems": max_items}
        print(f"▶️ Starting Apify Actor for '{keyword}' in '{location}'...")
        run = apify_client.actor('misceres/indeed-scraper').start(run_input=run_input)
        print("...Actor is running. Waiting for results...")
        actor_run = apify_client.run(run['id']).wait_for_finish()

        if actor_run['status'] != 'SUCCEEDED':
            print(f"❌ Actor run failed with status: {actor_run['status']}.")
            print(f"❌ Actor run failed with status: {actor_run['status']}.")
            return 0
        
        print("✅ Actor run SUCCEEDED. Storing results...")
        conn = get_db_connection()
        if not conn: return
        try:
            dataset_items = apify_client.dataset(actor_run['defaultDatasetId']).list_items().items
            count = 0
            for item in dataset_items:
                if insert_job_data(conn, item):
                    count += 1
            print(f"✅ Successfully stored/updated {count} jobs.")
            return count
        finally:
            conn.close()
    except Exception as e:
        print(f"❌ An unexpected error occurred during scraping: {e}")
        return 0

# --- 3. Execution Block (Only runs when you execute 'python job_scraper.py') ---
if __name__ == '__main__':
    print("--- Running Scraper Standalone ---")
    asyncio.run(scrape_and_store_jobs(
        keyword="Fullstack", 
        location="Hyderabad", 
        max_items=1
    ))
    print("--- Standalone Scraper Finished ---")