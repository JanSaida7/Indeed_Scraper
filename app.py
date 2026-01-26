"""
Flask application for the Indeed Job Scraper.

This module provides routes for:
- Serving the main dashboard.
- API access to stored jobs.
- API endpoint to trigger the scraper.
"""
import os
import asyncio
import psycopg2
import csv
import io
from flask import Flask, render_template, jsonify, request, make_response

# Import the functions and variables from our scraper module
from job_scraper import get_db_connection, scrape_and_store_jobs

# --- Flask Application ---
app = Flask(__name__)

def fetch_jobs_from_db():
    """Fetches all jobs from the database and returns them as a list of dicts."""
    conn = get_db_connection()
    jobs = []
    if not conn: return jobs
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT job_title, company_name, location, to_char(post_date, 'YYYY-MM-DD') as post_date, job_url FROM jobs ORDER BY extracted_at DESC;")
            column_names = [desc[0] for desc in cursor.description]
            jobs = [dict(zip(column_names, record)) for record in cursor.fetchall()]
    except Exception as e:
        print(f"Error fetching jobs: {e}")
    finally:
        if conn: conn.close()
    return jobs

@app.route('/')
def home():
    """Renders the main dashboard page."""
    return render_template('index.html')

@app.route('/api/jobs')
def get_jobs_api():
    """API endpoint to get all jobs from the database as JSON."""
    return jsonify(fetch_jobs_from_db())

@app.route('/api/scrape', methods=['POST'])
def scrape_jobs_api():
    """API endpoint to trigger the job scraper."""
    data = request.json
    keyword = data.get('keyword', 'Python')
    location = data.get('location', 'Remote')
    try:
        # Get the number from the form, default to 10
        max_items = int(data.get('maxItems', 10))
        # Enforce the limit: ensures the number is between 1 and 10
        max_items = max(1, min(max_items, 10)) 
    except (ValueError, TypeError):
        # If the input isn't a valid number, default to 10
        max_items = 10
    
    print(f"Received scrape request: keyword='{keyword}', location='{location}', max_items={max_items}")
    count = asyncio.run(scrape_and_store_jobs(keyword, location, max_items))
    return jsonify({
        "status": "success", 
        "message": f"Scraping completed. Found {count} jobs for '{keyword}'.",
        "job_count": count,
        "keyword": keyword
    })

@app.route('/api/export/json')
def export_json():
    """Export all jobs as a downloadable JSON file."""
    jobs = fetch_jobs_from_db()
    response = make_response(jsonify(jobs))
    response.headers['Content-Disposition'] = 'attachment; filename=jobs_export.json'
    response.mimetype = 'application/json'
    return response

@app.route('/api/export/csv')
def export_csv():
    """Export all jobs as a downloadable CSV file."""
    jobs = fetch_jobs_from_db()
    if not jobs:
        return jsonify({"message": "No jobs to export"}), 404

    # Create a CSV in memory
    si = io.StringIO()
    if jobs:
        cw = csv.DictWriter(si, fieldnames=jobs[0].keys())
        cw.writeheader()
        cw.writerows(jobs)
    
    output = make_response(si.getvalue())
    output.headers["Content-Disposition"] = "attachment; filename=jobs_export.csv"
    output.headers["Content-type"] = "text/csv"
    return output

if __name__ == '__main__':
    app.run(debug=True, port=5001)