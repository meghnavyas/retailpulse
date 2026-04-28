import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import random
import os

fake = Faker()
random.seed(42)
np.random.seed(42)

OUTPUT_DIR = "include/data/raw"
os.makedirs(OUTPUT_DIR, exist_ok=True)

NUM_CUSTOMERS = 2000
NUM_ORDERS = 8000
NUM_EVENTS = 40000
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2024, 6, 30)

CATEGORIES = ["Electronics", "Apparel", "Home & Kitchen", "Books", "Sports", "Beauty"]
CHANNELS = ["organic", "paid_search", "email", "social", "referral", "direct"]


def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))


def generate_customers():
    customers = []
    for i in range(NUM_CUSTOMERS):
        signup_date = random_date(START_DATE, END_DATE)
        customers.append({
            "customer_id": f"CUST_{i+1:05d}",
            "email": fake.email(),
            "country": random.choices(
                ["IN", "SG", "US", "GB", "AU"],
                weights=[40, 20, 20, 10, 10]
            )[0],
            "signup_date": signup_date.date(),
            "acquisition_channel": random.choice(CHANNELS),
            "age_group": random.choice(["18-24", "25-34", "35-44", "45-54", "55+"]),
            "is_premium": random.random() < 0.2,  # 20% premium customers
        })
    return pd.DataFrame(customers)


def generate_orders(customers_df):
    customer_ids = customers_df["customer_id"].tolist()
    signup_dates = dict(zip(customers_df["customer_id"], customers_df["signup_date"]))

    orders = []
    for i in range(NUM_ORDERS):
        customer_id = random.choice(customer_ids)
        # Orders must happen after signup
        order_min = datetime.combine(signup_dates[customer_id], datetime.min.time())
        order_date = random_date(max(order_min, START_DATE), END_DATE)

        quantity = random.randint(1, 5)
        unit_price = round(random.uniform(9.99, 499.99), 2)

        orders.append({
            "order_id": f"ORD_{i+1:07d}",
            "customer_id": customer_id,
            "order_date": order_date.date(),
            "category": random.choice(CATEGORIES),
            "quantity": quantity,
            "unit_price": unit_price,
            "total_amount": round(quantity * unit_price, 2),
            "status": random.choices(
                ["completed", "returned", "cancelled"],
                weights=[80, 12, 8]
            )[0],
            "payment_method": random.choice(["card", "upi", "netbanking", "wallet"]),
        })
    return pd.DataFrame(orders)


def generate_events(customers_df):
    customer_ids = customers_df["customer_id"].tolist()
    EVENT_TYPES = ["page_view", "product_view", "add_to_cart", "checkout_start",
                   "purchase", "search", "wishlist_add"]

    events = []
    for i in range(NUM_EVENTS):
        event_date = random_date(START_DATE, END_DATE)
        events.append({
            "event_id": f"EVT_{i+1:08d}",
            "customer_id": random.choice(customer_ids),
            "event_type": random.choices(
                EVENT_TYPES,
                weights=[30, 25, 18, 10, 7, 7, 3]
            )[0],
            "event_timestamp": event_date.strftime("%Y-%m-%d %H:%M:%S"),
            "session_id": f"SESS_{random.randint(1, 15000):06d}",
            "device": random.choice(["mobile", "desktop", "tablet"]),
            "page": fake.uri_path(),
        })
    return pd.DataFrame(events)


if __name__ == "__main__":
    print("Generating customers...")
    customers = generate_customers()
    customers.to_csv(f"{OUTPUT_DIR}/customers.csv", index=False)
    print(f"  {len(customers)} customers written.")

    print("Generating orders...")
    orders = generate_orders(customers)
    orders.to_csv(f"{OUTPUT_DIR}/orders.csv", index=False)
    print(f"  {len(orders)} orders written.")

    print("Generating events...")
    events = generate_events(customers)
    events.to_csv(f"{OUTPUT_DIR}/events.csv", index=False)
    print(f"  {len(events)} events written.")

    print(f"\nDone. Files in {OUTPUT_DIR}/")