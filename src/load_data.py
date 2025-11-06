import pandas as pd
import xml.etree.ElementTree as ET


def load_customers(file_path):
    """Load customer data from CSV file"""
    try:
        # Read the CSV file
        customers = pd.read_csv(file_path)

        # Clean up the data
        customers['customer_name'] = customers['customer_name'].str.strip()
        customers['region'] = customers['region'].str.strip()
        customers['mobile_number'] = customers['mobile_number'].astype(str)

        print(f"Loaded {len(customers)} customers")
        return customers

    except Exception as e:
        print(f"Error loading customers: {e}")
        return pd.DataFrame()


def load_orders(file_path):
    """Load order data from XML file"""
    try:
        # Parse the XML file
        tree = ET.parse(file_path)
        root = tree.getroot()

        orders_list = []
        seen_orders = set()  # To avoid duplicates

        # Go through each order in the XML
        for order in root.findall('order'):
            order_id = order.find('order_id').text

            # Only add each order once
            if order_id not in seen_orders:
                order_data = {
                    'order_id': order_id,
                    'mobile_number': order.find('mobile_number').text,
                    'order_date_time': order.find('order_date_time').text,
                    'total_amount': float(order.find('total_amount').text)
                }
                orders_list.append(order_data)
                seen_orders.add(order_id)

        # Convert to DataFrame
        orders = pd.DataFrame(orders_list)

        # Convert date to proper format
        orders['order_date_time'] = pd.to_datetime(orders['order_date_time'])
        orders['mobile_number'] = orders['mobile_number'].astype(str)

        print(f"Loaded {len(orders)} orders")
        return orders

    except Exception as e:
        print(f"Error loading orders: {e}")
        return pd.DataFrame()
