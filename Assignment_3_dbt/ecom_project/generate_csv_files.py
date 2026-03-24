import csv
import random
import os
from datetime import datetime, timedelta

# Створюємо папку seeds, якщо її немає
os.makedirs('seeds', exist_ok=True)

# Словники для генерації реалістичних даних
first_names = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda", "William", "Elizabeth",
               "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"]
last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez",
              "Hernandez", "Lopez", "Gonzalez", "Wilson"]
cities = ["Kyiv", "Lviv", "Odesa", "Dnipro", "London", "New York", "Berlin", "Paris", "Toronto", "Warsaw"]
categories = ["Electronics", "Clothing", "Home & Garden", "Sports", "Books"]
statuses = ["delivered", "shipped", "returned", "cancelled", "processing"]

# Налаштування кількості записів
NUM_USERS = 200
NUM_PRODUCTS = 40
NUM_ORDERS = 1000


# Допоміжна функція для генерації випадкових дат
def random_date(start_date, end_date):
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    return start_date + timedelta(days=random_number_of_days)


print("Починаю генерацію даних...")

# 1. Генерація Users (Клієнти)
users = []
for i in range(1, NUM_USERS + 1):
    fname = random.choice(first_names)
    lname = random.choice(last_names)
    users.append({
        "user_id": i,
        "first_name": fname,
        "last_name": lname,
        "email": f"{fname.lower()}.{lname.lower()}{i}@example.com",
        "city": random.choice(cities),
        "registered_at": random_date(datetime(2023, 1, 1), datetime(2023, 12, 31)).strftime("%Y-%m-%d %H:%M:%S")
    })

with open('seeds/raw_users.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=users[0].keys())
    writer.writeheader()
    writer.writerows(users)

# 2. Генерація Products (Товари)
products = []
for i in range(1, NUM_PRODUCTS + 1):
    category = random.choice(categories)
    price = round(random.uniform(10.0, 500.0), 2)
    products.append({
        "product_id": i,
        "name": f"{category[:-1]} Item {i}",
        "category": category,
        "price": price,
        "cost": round(price * random.uniform(0.4, 0.7), 2)  # Собівартість для розрахунку маржі
    })

with open('seeds/raw_products.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=products[0].keys())
    writer.writeheader()
    writer.writerows(products)

# 3. Генерація Promotions (Знижки)
promotions = [
    {"promo_id": 1, "promo_name": "WINTER_SALE", "discount_pct": 0.15},
    {"promo_id": 2, "promo_name": "SPRING_PROMO", "discount_pct": 0.10},
    {"promo_id": 3, "promo_name": "BLACK_FRIDAY", "discount_pct": 0.30},
    {"promo_id": 4, "promo_name": "VIP_DISCOUNT", "discount_pct": 0.20}
]

with open('seeds/raw_promotions.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=promotions[0].keys())
    writer.writeheader()
    writer.writerows(promotions)

# 4. Генерація Orders (Замовлення)
orders = []
start_date_orders = datetime(2024, 1, 1)
end_date_orders = datetime(2024, 12, 31)

for i in range(1, NUM_ORDERS + 1):
    order_date = random_date(start_date_orders, end_date_orders)
    # Дата оновлення статусу (важливо для інкрементальних моделей dbt)
    updated_at = order_date + timedelta(days=random.randint(1, 10), hours=random.randint(1, 23))

    orders.append({
        "order_id": i,
        "user_id": random.randint(1, NUM_USERS),
        "status": random.choices(statuses, weights=[60, 20, 10, 5, 5])[0],
        "promo_id": random.choice([None, 1, 2, 3, 4]) if random.random() > 0.7 else "",
        "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
        "updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S")
    })

with open('seeds/raw_orders.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=orders[0].keys())
    writer.writeheader()
    writer.writerows(orders)

# 5. Генерація Order Items (Деталі замовлень)
order_items = []
item_id = 1
for order in orders:
    # Від 1 до 5 товарів у кожному замовленні
    num_items_in_order = random.randint(1, 5)

    # Вибираємо унікальні товари для одного замовлення
    selected_products = random.sample(products, num_items_in_order)

    for prod in selected_products:
        order_items.append({
            "order_item_id": item_id,
            "order_id": order["order_id"],
            "product_id": prod["product_id"],
            "quantity": random.randint(1, 3),
            "unit_price": prod["price"]  # Ціна на момент покупки
        })
        item_id += 1

with open('seeds/raw_order_items.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=order_items[0].keys())
    writer.writeheader()
    writer.writerows(order_items)

print("Готово! Перевір папку 'seeds'. Там лежить 5 CSV файлів.")