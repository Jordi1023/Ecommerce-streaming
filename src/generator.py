import random
import uuid
from datetime import datetime, timezone
from faker import Faker

fake = Faker()

CATEGORIES = {
    "Electronics": ["Smartphone", "Laptop", "Wireless Headphones", "Smartwatch", "4K Monitor"],
    "Home & Kitchen": ["Air Fryer", "Coffee Maker", "Robot Vacuum", "Blender", "Microwave"],
    "Fashion": ["Running Shoes", "Denim Jacket", "Sunglasses", "Leather Backpack", "T-Shirt"],
    "Beauty & Health": ["Facial Serum", "Electric Toothbrush", "Sunscreen SPF 50", "Perfume"],
    "Sports": ["Yoga Mat", "Dumbbells 10kg", "Resistance Bands", "Cycling Helmet"]
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "Yape", "Plin", "PayPal"]
DEVICE_TYPES = ["Mobile App", "Web Browser", "Tablet"]
CITIES_PERU = ["Lima", "Arequipa", "Trujillo", "Cusco", "Chiclayo", "Piura", "Huancayo"]

def generate_ecommerce_event():
    """Genera un evento individual de transaccion de e-commerce sintetica."""
    category = random.choice(list(CATEGORIES.keys()))
    product_name = random.choice(CATEGORIES[category])
    
    # Precios referenciales aproximados por categoria
    price_ranges = {
        "Electronics": (80.0, 1200.0),
        "Home & Kitchen": (30.0, 350.0),
        "Fashion": (20.0, 150.0),
        "Beauty & Health": (15.0, 90.0),
        "Sports": (15.0, 120.0)
    }
    min_p, max_p = price_ranges[category]
    price = round(random.uniform(min_p, max_p), 2)
    quantity = random.choices([1, 2, 3, 4], weights=[0.7, 0.2, 0.07, 0.03])[0]
    total_amount = round(price * quantity, 2)
    
    event = {
        "event_id": str(uuid.uuid4()),
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "user_id": f"USR-{random.randint(1000, 9999)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event_type": random.choices(["purchase", "add_to_cart", "cart_abandoned"], weights=[0.75, 0.15, 0.10])[0],
        "product_name": product_name,
        "category": category,
        "unit_price": price,
        "quantity": quantity,
        "total_amount": total_amount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "device": random.choice(DEVICE_TYPES),
        "city": random.choice(CITIES_PERU),
        "user_email": fake.email()
    }
    return event