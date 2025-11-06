import pandas as pd
from .load_data import load_customers, load_orders
from .db_approach import get_db_engine, setup_database, get_sql_kpis
from .in_memory_approach import merge_data, calculate_kpis

print("Starting data processing...")


def main():
    # Load data from files
    print("Loading customer data...")
    customers = load_customers('data/task_DE_new_customers.csv')

    print("Loading order data...")
    orders = load_orders('data/task_DE_new_orders.xml')

    # Check if data loaded successfully
    if customers.empty:
        print("Error: No customer data found!")
        return
    if orders.empty:
        print("Error: No order data found!")
        return

    print(f"Loaded {len(customers)} customers and {len(orders)} orders")

    # --- Approach A: Database (SQL) Approach ---
    print("\n" + "="*50)
    print("RUNNING DATABASE (SQL) APPROACH")
    print("="*50)

    engine = get_db_engine()
    if engine:
        print("Database connection successful!")
        if setup_database(engine, customers, orders):
            print("Database setup complete!")
            sql_results = get_sql_kpis(engine)
            if sql_results:
                print("\nDatabase KPI Results:")
                for metric_name, data in sql_results.items():
                    print(f"\n{metric_name.upper().replace('_', ' ')}:")
                    print("-" * 30)
                    if not data.empty:
                        print(data.to_string(index=False))
                    else:
                        print("No data available")
            else:
                print("Failed to calculate SQL KPIs")
        else:
            print("Database setup failed")
    else:
        print("Failed to connect to database. Skipping SQL approach.")

    # --- Approach B: In-Memory (Pandas) Approach ---
    print("\n" + "="*50)
    print("RUNNING IN-MEMORY (PANDAS) APPROACH")
    print("="*50)

    # Merge the data
    print("Merging customer and order data...")
    merged_data = merge_data(customers, orders)

    if merged_data.empty:
        print("Error: Could not merge data!")
        return

    print(f"Successfully merged data: {len(merged_data)} records")

    # Calculate KPIs
    print("Calculating business metrics...")
    results = calculate_kpis(merged_data)

    # Display results
    print("\nIn-Memory KPI Results:")
    for metric_name, data in results.items():
        print(f"\n{metric_name.upper().replace('_', ' ')}:")
        print("-" * 30)
        if not data.empty:
            print(data.to_string(index=False))
        else:
            print("No data available")


if __name__ == "__main__":
    main()
