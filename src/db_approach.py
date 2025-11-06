import os
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import logging

# Load .env file to get credentials
load_dotenv()


def get_db_engine():
    """
    Creates a secure SQLAlchemy engine using credentials from .env file.
    First connects without database to create it if needed.
    """
    try:
        db_user = os.getenv('DB_USER')
        db_pass = os.getenv('DB_PASSWORD')
        db_host = os.getenv('DB_HOST')
        db_port = os.getenv('DB_PORT', '3306')
        db_name = os.getenv('DB_NAME')

        if not all([db_user, db_pass, db_host, db_name]):
            logging.error("Database credentials not fully set in .env file.")
            return None

        # First, try to connect without specifying a database to test credentials
        base_engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/"
        )

        # Test connection and create database if it doesn't exist
        with base_engine.connect() as conn:
            conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {db_name}"))
            logging.info(f"Database '{db_name}' is ready.")

        # Now connect to the specific database
        engine = create_engine(
            f"mysql+pymysql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
        )

        # Test final connection
        with engine.connect() as conn:
            logging.info("Database connection successful.")
        return engine
    except Exception as e:
        logging.error(f"Failed to create database engine: {e}")
        return None


def setup_database(engine, customers_df, orders_df):
    """
    Creates tables and loads data into the MySQL database[cite: 35].
    It's designed to be idempotent (using if_exists='replace').
    For a production pipeline, you'd use if_exists='append' for daily data[cite: 19].
    """
    try:
        # Load customers table
        customers_df.to_sql('customers', con=engine, if_exists='replace', index=False, dtype={
            'customer_id': sqlalchemy.types.VARCHAR(length=50),
            'customer_name': sqlalchemy.types.VARCHAR(length=100),
            'mobile_number': sqlalchemy.types.VARCHAR(length=20),
            'region': sqlalchemy.types.VARCHAR(length=50)
        })
        logging.info("Loaded 'customers' table.")

        # Load orders table
        orders_df.to_sql('orders', con=engine, if_exists='replace', index=False, dtype={
            'order_id': sqlalchemy.types.VARCHAR(length=50),
            'mobile_number': sqlalchemy.types.VARCHAR(length=20),
            'order_date_time': sqlalchemy.types.DateTime(timezone=True),
            'total_amount': sqlalchemy.types.Float()
        })
        logging.info("Loaded 'orders' table.")

        # Add indexes and keys for performance
        with engine.connect() as conn:
            conn.execute(
                text('ALTER TABLE `customers` ADD PRIMARY KEY (`customer_id`);'))
            # Index on mobile_number for efficient joins
            conn.execute(
                text('CREATE INDEX `idx_mobile_number` ON `customers` (`mobile_number`);'))

            conn.execute(
                text('ALTER TABLE `orders` ADD PRIMARY KEY (`order_id`);'))
            # Foreign key to customer
            conn.execute(text(
                'ALTER TABLE `orders` ADD CONSTRAINT `fk_customer_mobile` FOREIGN KEY (`mobile_number`) REFERENCES `customers` (`mobile_number`);'))
            # Index on date for time-based queries
            conn.execute(
                text('CREATE INDEX `idx_order_date` ON `orders` (`order_date_time`);'))

        logging.info("Database setup complete with keys and indexes.")
        return True

    except Exception as e:
        logging.error(f"Error setting up database: {e}")
        return False


def get_sql_kpis(engine):
    """
    Uses SQL queries to compute each required KPI[cite: 38].
    """
    kpis = {}

    # Using a fixed date for "today" for reproducible 30-day KPI
    today_str = '2025-11-06T00:00:00Z'

    try:
        with engine.connect() as conn:

            # KPI 1: Repeat Customers
            sql_repeat = text("""
                SELECT c.customer_name, COUNT(o.order_id) as order_count
                FROM customers c
                JOIN orders o ON c.mobile_number = o.mobile_number
                GROUP BY c.customer_id, c.customer_name
                HAVING order_count > 1;
            """)
            kpis['repeat_customers'] = pd.read_sql(sql_repeat, conn)

            # KPI 2: Monthly Order Trends
            sql_monthly = text("""
                SELECT 
                    DATE_FORMAT(order_date_time, '%Y-%m') as order_month,
                    COUNT(order_id) as total_orders,
                    SUM(total_amount) as total_revenue
                FROM orders
                GROUP BY order_month
                ORDER BY order_month;
            """)
            kpis['monthly_trends'] = pd.read_sql(
                sql_monthly, conn).set_index('order_month')

            # KPI 3: Regional Revenue
            sql_regional = text("""
                SELECT c.region, SUM(o.total_amount) as regional_revenue
                FROM customers c
                JOIN orders o ON c.mobile_number = o.mobile_number
                GROUP BY c.region;
            """)
            kpis['regional_revenue'] = pd.read_sql(sql_regional, conn)

            # KPI 4: Top Customers by Spend (Last 30 Days)
            # Using parameterized query to prevent SQL injection
            sql_top_spenders = text(f"""
                SELECT c.customer_name, SUM(o.total_amount) as recent_spend
                FROM customers c
                JOIN orders o ON c.mobile_number = o.mobile_number
                WHERE o.order_date_time >= (STR_TO_DATE(:today, '%Y-%m-%dT%H:%i:%sZ') - INTERVAL 30 DAY)
                  AND o.order_date_time <= STR_TO_DATE(:today, '%Y-%m-%dT%H:%i:%sZ')
                GROUP BY c.customer_id, c.customer_name
                ORDER BY recent_spend DESC
                LIMIT 10;
            """)
            kpis['top_spenders'] = pd.read_sql(
                sql_top_spenders, conn, params={"today": today_str})

        logging.info("Successfully calculated all SQL KPIs.")

    except Exception as e:
        logging.error(f"Error calculating SQL KPIs: {e}")

    return kpis
