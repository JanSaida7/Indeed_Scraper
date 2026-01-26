import asyncio
import json
import os
import hashlib
import re
import datetime

import psycopg2
from psycopg2 import sql
import csv
import pandas as pd

# --- Configuration for Apify and PostgreSQL ---

# IMPORTANT: Your Apify API token.
# LOADED FROM ENVIRONMENT VARIABLE FOR SECURITY
APIFY_TOKEN = os.getenv('APIFY_TOKEN')

# PostgreSQL Database Configuration
# MODIFIED: Using 'indeed_jobs_db' to match the Flask App
DB_CONFIG = {
    'dbname': 'indeed_jobs_db',
    'user': 'postgres',
    'password': 'sst1234489',
    'host': 'localhost',
    'port': '5432'
}


# --- PostgreSQL Database Functions ---

def get_db_connection():
    """Establishes and returns a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("Successfully connected to PostgreSQL database.")
        return conn
    except Exception as e:
        print(f"Error connecting to database: {e}")
        print("Please check your DB_CONFIG details and ensure PostgreSQL server is running.")
        return None


def create_jobs_table(conn):
    """Creates the 'jobs' table in the database if it doesn't exist."""
    cursor = conn.cursor()
    try:
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
        print("Table 'jobs' checked/created successfully.")
    except Exception as e:
        conn.rollback()
        print(f"Error creating table: {e}")
    finally:
        cursor.close()


def insert_job_data(conn, job_data):
    """
    Inserts a single job record into the 'jobs' table.
    Uses ON CONFLICT DO UPDATE to handle existing records based on 'job_url'.
    This ensures idempotency and keeps job data fresh.
    """
    cursor = conn.cursor()
    try:
        if not job_data.get('job_url'):
            print(f"Warning: Job data for '{job_data.get('job_title', 'Unknown')}' has no URL; skipping insertion.")
            return False, None

        # Generate a unique job_id if not provided by the scraper, using URL hash as a fallback.
        if not job_data.get('job_id'):
            job_data['job_id'] = hashlib.md5(job_data['job_url'].encode('utf-8')).hexdigest()

        # --- IMPROVED DATE PARSING LOGIC ---
        parsed_post_date = None
        raw_post_date = job_data.get('post_date')
        if raw_post_date:
            raw_post_date_lower = str(raw_post_date).lower().strip()
            today = datetime.date.today()

            if "today" in raw_post_date_lower or "just posted" in raw_post_date_lower:
                parsed_post_date = today
            elif "yesterday" in raw_post_date_lower:
                parsed_post_date = today - datetime.timedelta(days=1)
            elif "day" in raw_post_date_lower and "ago" in raw_post_date_lower:
                match = re.search(r'(\d+)\s*days?\s*ago', raw_post_date_lower)
                if match:
                    try:
                        days_ago = int(match.group(1))
                        parsed_post_date = today - datetime.timedelta(
                            days=days_ago)
                    except ValueError:
                        print(f"Warning: Could not convert extracted number to integer for '{raw_post_date}'.")
                else:
                    print(f"Warning: Regex could not match 'day(s) ago' pattern for '{raw_post_date}'.")
            elif "hour" in raw_post_date_lower and "ago" in raw_post_date_lower:
                parsed_post_date = today
            else:
                # Final fallback: Try parsing as an ISO date 'YYYY-MM-DD'
                try:
                    parsed_post_date = datetime.datetime.strptime(raw_post_date,
                                                                  '%Y-%m-%d').date()
                except ValueError:
                    print(f"Warning: Unhandled or unparsable post_date format '{raw_post_date}'. Storing as NULL.")
        # --- END IMPROVED DATE PARSING LOGIC ---

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
                extracted_at = EXCLUDED.extracted_at
            RETURNING job_id;
        """)

        cursor.execute(insert_query, (
            job_data.get('job_id'),
            job_data.get('job_title'),
            job_data.get('company_name'),
            job_data.get('location'),
            parsed_post_date,
            job_data.get('job_description'),
            job_data.get('salary_info'),
            job_data.get('job_url'),
            job_data.get('source_website'),
            datetime.datetime.now(datetime.timezone.utc).isoformat()
        ))
        inserted_id = cursor.fetchone()[0]
        conn.commit()
        return True, inserted_id
    except psycopg2.IntegrityError as e:
        conn.rollback()
        print(f"PostgreSQL Integrity Error (likely duplicate URL, updated existing): {e}")
        return False, None
    except Exception as e:
        conn.rollback()
        print(f"Error inserting job data: {e}")
        import traceback
        traceback.print_exc()
        return False, None
    finally:
        cursor.close()


def fetch_all_jobs(conn):
    """
    Fetches all job records from the 'jobs' table and returns them as a list of dictionaries.
    Converts datetime objects to ISO format strings for JSON serialization.
    """
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT * FROM jobs ORDER BY extracted_at DESC;")
        column_names = [desc[0] for desc in cursor.description]
        records = cursor.fetchall()
        jobs_list = []
        for row in records:
            job_dict = dict(zip(column_names, row))
            for k, v in job_dict.items():
                if isinstance(v, (datetime.datetime, datetime.date)):
                    job_dict[k] = v.isoformat()
            jobs_list.append(job_dict)
        return jobs_list
    except Exception as e:
        print(f"Error fetching jobs from database: {e}")
        import traceback
        traceback.print_exc()
        return []
    finally:
        cursor.close()


# --- Apify Scraping Logic ---

from apify_client import ApifyClientAsync
from apify_shared.consts import ActorJobStatus


async def scrape_indeed_jobs_apify(
        apify_token: str,
        search_keyword: str,
        location: str,
        country_code: str,
        max_items: int = 50
):
    """
    Scrapes job data from Indeed using a pre-built Apify Actor.
    """
    if not apify_token:
        print("Error: Apify API token is empty. Please ensure it's provided.")
        return []

    try:
        apify_client = ApifyClientAsync(apify_token)
        print("ApifyClient initialized.")

        indeed_scraper_actor_id = 'misceres/indeed-scraper'
        print(
            f"Running Apify Actor: {indeed_scraper_actor_id} for '{search_keyword}' in '{location}', country '{country_code}'...")

        run_input = {
            "position": search_keyword,
            "location": location,
            "country": country_code.upper(),
            "maxItems": max_items,
            "proxyConfiguration": {
                "useApifyProxy": True,
                "apifyProxyGroups": ["RESIDENTIAL"]
            }
        }

        actor_run = await apify_client.actor(indeed_scraper_actor_id).call(run_input=run_input)

        if not actor_run or actor_run['status'] != ActorJobStatus.SUCCEEDED:
            print(f"Actor '{indeed_scraper_actor_id}' run failed or did not succeed.")
            if actor_run:
                print(f"Actor run status: {actor_run['status']}")
            return []

        print(f"Actor run completed successfully. Run ID: {actor_run['id']}")
        print(f"View run details: https://console.apify.com/actors/runs/{actor_run['id']}")

        if 'defaultDatasetId' not in actor_run or not actor_run['defaultDatasetId']:
            print("No default dataset found for this Actor run.")
            return []

        dataset_id = actor_run['defaultDatasetId']
        dataset_client = apify_client.dataset(dataset_id)

        scraped_items_page = await dataset_client.list_items()
        scraped_items = scraped_items_page.items

        if not scraped_items:
            print("No job postings found in the dataset.")
            return []

        print(f"Fetched {len(scraped_items)} items from the dataset.")
        return scraped_items

    except Exception as e:
        print(f"An unexpected error occurred during Apify scraping: {e}")
        import traceback
        traceback.print_exc()
        return []


# --- Main Application Workflow ---

async def run_job_scraper_workflow(search_keyword: str, location: str, country_code: str, max_items: int = 50):
    """
    Orchestrates the entire job scraping and storage workflow.
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database. Cannot proceed with scraping/storage.")
        return

    try:
        create_jobs_table(conn)

        scraped_jobs_raw = await scrape_indeed_jobs_apify(APIFY_TOKEN, search_keyword, location, country_code,
                                                          max_items)

        if not scraped_jobs_raw:
            print("No jobs were scraped or an error occurred during scraping. No data to store.")
            return

        print(f"Attempting to store {len(scraped_jobs_raw)} jobs into PostgreSQL...")
        stored_count = 0
        for item in scraped_jobs_raw:
            job_data = {
                "job_id": item.get('id'),
                "job_title": item.get('positionName'), 
                "company_name": item.get('company'),
                "location": item.get('location'),
                "post_date": item.get('postedAt'),
                "job_description": item.get('description'),
                "salary_info": item.get('salary'),
                "job_url": item.get('url'),
                "source_website": "Indeed",
                "extracted_at": datetime.datetime.now(datetime.timezone.utc).isoformat()
            }
            success, _ = insert_job_data(conn, job_data)
            if success:
                stored_count += 1
        print(f"Successfully stored/updated {stored_count} job postings in PostgreSQL.")

    except Exception as e:
        print(f"An error occurred during the main workflow: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")


# --- Data Export Functions ---

def export_data(format_type: str, filename: str):
    """
    Fetches all job data from the database and exports it.
    """
    conn = get_db_connection()
    if not conn:
        print("Failed to connect to the database for export. Exiting export process.")
        return

    try:
        print(f"Exporting data to {format_type.upper()} format to '{filename}'...")
        jobs = fetch_all_jobs(conn)

        if not jobs:
            print("No data found in the database to export.")
            return

        df = pd.DataFrame(jobs)

        if format_type.lower() == 'csv':
            df.to_csv(filename, index=False, encoding='utf-8')
            print(f"Data successfully exported to {filename}")
        elif format_type.lower() == 'json':
            df.to_json(filename, orient='records', indent=4)
            print(f"Data successfully exported to {filename}")
        else:
            print(f"Unsupported export format: {format_type}. Please choose 'csv' or 'json'.")
    except Exception as e:
        print(f"An error occurred during data export: {e}")
        import traceback
        traceback.print_exc()
    finally:
        if conn:
            conn.close()


# --- Main Execution Block ---

if __name__ == '__main__':
    SEARCH_KEYWORD = 'Software Developer' 
    LOCATION = 'Chennai'
    COUNTRY_CODE = 'IN'
    MAX_ITEMS_TO_SCRAPE = 5

    print("--- Starting Job Scraper Application ---")
    asyncio.run(run_job_scraper_workflow(SEARCH_KEYWORD, LOCATION, COUNTRY_CODE, MAX_ITEMS_TO_SCRAPE))
