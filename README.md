# Indeed Job Scraper

A web application to scrap, store, and view job listings from Indeed using Apify and PostgreSQL. The application scans for jobs based on keywords and locations, stores them in a database to prevent duplicates, and organizes them in a user-friendly dashboard.

## Features

-   **Job Scraping**: Automated scraping of job listings from Indeed via Apify.
-   **Dashboard**: A clean, responsive interface to view job opportunities.
-   **Database Storage**: Persistent storage using PostgreSQL to handle large datasets and avoid re-scraping the same jobs.
-   **Search & Filter**: Trigger new scrapes directly from the UI with custom keywords and locations.
-   **Export Data**: Download your job collection as JSON or CSV files.
-   **API Access**: RESTful endpoints to fetch job data and trigger scraping programmatically.

## Prerequisites

Before you begin, ensure you have the following installed:
-   Python 3.8+
-   [PostgreSQL](https://www.postgresql.org/download/)
-   An [Apify](https://apify.com/) account (for the API token)

## Setup

### 1. Install Dependencies

Clone the repository and install the required Python packages:

```bash
pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the root directory and add your credentials:

```env
APIFY_TOKEN=your_apify_api_token
DB_NAME=your_db_name
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
```

### 3. Database Initialization

Ensure your PostgreSQL server is running and the database specified in `DB_NAME` exists.
You can use the `standalone_scraper.py` script to initialize the database table and test the scraper:

```bash
python standalone_scraper.py
```
*Note: This will create the `jobs` table if it doesn't exist and perform a test scrape.*

## Usage

### Run the Web Application

Start the Flask application:

```bash
python app.py
```

Open your browser and navigate to: `http://127.0.0.1:5001`

### Using the Dashboard
-   **Home**: View the latest job listings.
-   **Scrape Jobs**: Use the form at the top to enter a "Job Role" (e.g., Python Developer) and "Location" (e.g., Remote). Click "Scrape Jobs" to fetch new data.
-   **Export**: Use the "Export JSON" or "Export CSV" buttons to download your data.

## API Endpoints

The application exposes the following API endpoints:

-   `GET /api/jobs`: Retrieve stored jobs.
    -   Query Params: `limit` (optional)
-   `POST /api/scrape`: Trigger a scraping job.
    -   Body: `{"keyword": "...", "location": "...", "maxItems": 10}`
-   `GET /api/export/json`: Download jobs as JSON.
-   `GET /api/export/csv`: Download jobs as CSV.

## Project Structure

-   `app.py`: Main Flask application entry point.
-   `job_scraper.py`: Core logic for scraping (via Apify) and database operations.
-   `standalone_scraper.py`: a versatile script for running scraping jobs independent of the web server.
-   `templates/index.html`: The HTML template for the web dashboard.