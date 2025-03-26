# NHL Play-by-Play Scraper

This project scrapes NHL play-by-play data from the NHL Stats API, parses event-level data, and saves it into CSV or SQLite. Eventually, it will migrate data storage to PostgreSQL.

## Goals

- Scrape live NHL JSON play-by-play data
- Parse and structure the data using pandas
- Store structured data in SQLite, with PostgreSQL planned for later

## Tech Stack

- **Python** (with requests, pandas, and SQLAlchemy)
- **SQLite** (for initial data storage)
- **Git & GitHub** for version control

## Project Setup

Create and activate your virtual environment:

```bash
python -m venv venv
source venv/bin/activate # macOS/Linux
.\venv\Scripts\activate  # Windows
pip install -r requirements.txt
