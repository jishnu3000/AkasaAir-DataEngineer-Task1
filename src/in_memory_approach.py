import pandas as pd


def merge_data(customers, orders):
    """Combine customer and order data"""
    try:
        # Make sure mobile numbers are the same type
        customers['mobile_number'] = customers['mobile_number'].astype(str)
        orders['mobile_number'] = orders['mobile_number'].astype(str)

        # Join the data on mobile number
        merged = pd.merge(customers, orders, on='mobile_number', how='inner')

        print(f"Merged data: {len(merged)} records")
        return merged

    except Exception as e:
        print(f"Error merging data: {e}")
        return pd.DataFrame()


def calculate_kpis(merged_data):
    """Calculate business metrics"""
    results = {}

    try:
        # 1. Repeat Customers (customers with more than 1 order)
        order_counts = merged_data.groupby('customer_name')['order_id'].count()
        repeat_customers = order_counts[order_counts > 1]

        repeat_df = pd.DataFrame({
            'customer_name': repeat_customers.index,
            'number_of_orders': repeat_customers.values
        })
        results['repeat_customers'] = repeat_df

        # 2. Monthly Sales Trends
        merged_data['month'] = merged_data['order_date_time'].dt.to_period('M')
        monthly_sales = merged_data.groupby('month').agg({
            'order_id': 'count',
            'total_amount': 'sum'
        }).reset_index()
        monthly_sales.columns = ['month', 'total_orders', 'total_revenue']
        monthly_sales['month'] = monthly_sales['month'].astype(str)
        results['monthly_trends'] = monthly_sales

        # 3. Revenue by Region
        regional_revenue = merged_data.groupby(
            'region')['total_amount'].sum().reset_index()
        regional_revenue.columns = ['region', 'total_revenue']
        regional_revenue = regional_revenue.sort_values(
            'total_revenue', ascending=False)
        results['regional_revenue'] = regional_revenue

        # 4. Top 10 Customers by Spending (Last 30 Days)
        # Use a fixed date for consistency
        today = pd.to_datetime('2025-11-06')
        thirty_days_ago = today - pd.Timedelta(days=30)

        recent_orders = merged_data[
            (merged_data['order_date_time'] >= thirty_days_ago) &
            (merged_data['order_date_time'] <= today)
        ]

        top_spenders = recent_orders.groupby('customer_name')[
            'total_amount'].sum()
        top_spenders = top_spenders.nlargest(10).reset_index()
        top_spenders.columns = ['customer_name', 'total_spent']
        results['top_spenders'] = top_spenders

        print("Successfully calculated all metrics")
        return results

    except Exception as e:
        print(f"Error calculating metrics: {e}")
        return {}
