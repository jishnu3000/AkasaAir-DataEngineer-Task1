# Akasa Air Data Engineer Task 1

## What this project does:

1. Loads customer data from a CSV file
2. Loads order data from an XML file
3. Combines the data together
4. Calculates business metrics like:
   - Customers who made multiple orders
   - Monthly sales trends
   - Revenue by region
   - Top spending customers

## How to run:

1. Make sure you have Python and pandas installed
2. Open command prompt in the project folder
3. Run: `python src\main.py`

## Files:

- `src/main.py` - Main program that runs everything
- `src/load_data.py` - Loads data from CSV and XML files
- `src/in_memory_approach.py` - Merges data and calculates metrics
- `data/` - Contains the data files

## Requirements:

- Python 3.13
- pandas
- lxml
- sqlalchemy
- pymysql
- python-dotenv
- tabulate

Install pandas with: `pip install -r requirements.txt`
