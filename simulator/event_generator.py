# simulator/event_generator.py
# Generates realistic fake user events using the Faker library
# Timestamp format: 'YYYY-MM-DD HH:MM:SS' — compatible with Snowflake TIMESTAMP_TZ

import random
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

CATEGORIES      = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']
PAGES           = ['home', 'product', 'search', 'cart', 'checkout', 'account', 'wishlist']
DEVICES         = ['mobile', 'desktop', 'tablet']
COUNTRIES       = ['US', 'UK', 'CA', 'AU', 'DE', 'FR', 'JP', 'BR']
PAYMENT_METHODS = ['credit_card', 'paypal', 'apple_pay', 'google_pay', 'debit_card']
REFERRERS       = ['google', 'facebook', 'direct', 'email', 'instagram']

# 200 products generated once at import time
PRODUCTS = [
    {
        'id':       f'PROD_{i:04d}',
        'name':     fake.word().title(),
        'price':    round(random.uniform(9.99, 499.99), 2),
        'category': random.choice(CATEGORIES),
    }
    for i in range(1, 201)
]


def _now() -> str:
    """Return current UTC time as a Snowflake-compatible timestamp string."""
    return datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')


def generate_click_event(user_id: str, session_id: str) -> dict:
    """
    Matches RAW_CLICKS schema:
    event_id, event_type, user_id, session_id, product_id, product_name,
    product_category, page, device, country, referrer, price_seen, event_timestamp
    (INGESTED_AT is DEFAULT in Snowflake — do not send)
    """
    product = random.choice(PRODUCTS)
    return {
        'event_id':         str(uuid.uuid4()),
        'event_type':       'click',
        'user_id':          user_id,
        'session_id':       session_id,
        'product_id':       product['id'],
        'product_name':     product['name'],
        'product_category': product['category'],
        'page':             random.choice(PAGES),
        'device':           random.choice(DEVICES),
        'country':          random.choice(COUNTRIES),
        'referrer':         random.choice(REFERRERS),
        'price_seen':       product['price'],
        'event_timestamp':  _now(),
    }


def generate_session_event(user_id: str) -> dict:
    """
    Matches RAW_SESSIONS schema:
    event_id, session_id, user_id, session_start, duration_seconds,
    pages_visited, device, country, is_new_user, event_timestamp
    (INGESTED_AT is DEFAULT in Snowflake — do not send)
    """
    now = _now()
    return {
        'event_id':         str(uuid.uuid4()),
        'session_id':       str(uuid.uuid4()),
        'user_id':          user_id,
        'session_start':    now,
        'duration_seconds': random.randint(30, 1800),
        'pages_visited':    random.randint(1, 15),
        'device':           random.choice(DEVICES),
        'country':          random.choice(COUNTRIES),
        'is_new_user':      random.random() < 0.3,
        'event_timestamp':  now,
    }


def generate_purchase_event(user_id: str, session_id: str) -> dict:
    """
    Matches RAW_PURCHASES schema:
    event_id, order_id, user_id, session_id, product_id, product_name,
    product_category, quantity, unit_price, total_amount, payment_method,
    country, is_fraudulent, event_timestamp
    (INGESTED_AT is DEFAULT in Snowflake — do not send)
    """
    product  = random.choice(PRODUCTS)
    quantity = random.randint(1, 5)
    return {
        'event_id':         str(uuid.uuid4()),
        'order_id':         str(uuid.uuid4()),
        'user_id':          user_id,
        'session_id':       session_id,
        'product_id':       product['id'],
        'product_name':     product['name'],
        'product_category': product['category'],
        'quantity':         quantity,
        'unit_price':       product['price'],
        'total_amount':     round(product['price'] * quantity, 2),
        'payment_method':   random.choice(PAYMENT_METHODS),
        'country':          random.choice(COUNTRIES),
        'is_fraudulent':    random.random() < 0.02,
        'event_timestamp':  _now(),
    }