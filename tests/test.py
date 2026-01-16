"""
Test script for mini_rdbms database engine - MULTIPLE TABLES DEMO
"""
from src.database import Database

print("Testing mini_rdbms Database with Multiple Tables...")
print("=" * 50)

# 1. Create database instance
print("1. Creating database instance...")
db = Database()
print("   Database created (data/ folder)")

# 2. Create MULTIPLE tables
print("\n2. Creating multiple tables...")

tables_to_create = [
    ("users", ["id", "name", "email", "age"]),
    ("products", ["product_id", "name", "price", "category"]),
    ("orders", ["order_id", "user_id", "product_id", "quantity", "total"])
]

for table_name, columns in tables_to_create:
    result = db.create_table(table_name, columns)
    print(f"   {table_name}: {result}")

# 3. Insert data into ALL tables
print("\n3. Inserting sample data into all tables...")

# Insert users
users = [
    [1, "Alice Johnson", "alice@example.com", 28],
    [2, "Bob Smith", "bob@example.com", 35],
    [3, "Charlie Brown", "charlie@example.com", 22]
]

print("   Inserting users...")
for user in users:
    db.insert("users", user)

# Insert products
products = [
    [101, "Laptop", 999.99, "Electronics"],
    [102, "Mouse", 29.99, "Accessories"],
    [103, "Keyboard", 79.99, "Accessories"],
    [104, "Monitor", 299.99, "Electronics"]
]

print("   Inserting products...")
for product in products:
    db.insert("products", product)

# Insert orders
orders = [
    [1001, 1, 101, 1, 999.99],
    [1002, 1, 102, 2, 59.98],
    [1003, 2, 103, 1, 79.99],
    [1004, 3, 104, 1, 299.99]
]

print("   Inserting orders...")
for order in orders:
    db.insert("orders", order)

# 4. Display all data
print("\n4. Displaying all database contents...")

print("\n   USERS table:")
all_users = db.select_all("users")
for user in all_users:
    print(f"     ID: {user[0]}, Name: {user[1]}, Email: {user[2]}, Age: {user[3]}")

print("\n   PRODUCTS table:")
all_products = db.select_all("products")
for product in all_products:
    print(f"     ID: {product[0]}, Name: {product[1]}, Price: ${product[2]}, Category: {product[3]}")

print("\n   ORDERS table:")
all_orders = db.select_all("orders")
for order in all_orders:
    print(f"     Order: {order[0]}, User: {order[1]}, Product: {order[2]}, Qty: {order[3]}, Total: ${order[4]}")

# 5. Database summary
print("\n5. Database Summary:")
print("=" * 50)

import os
import json

tables_count = 0
total_rows = 0

for filename in os.listdir("data"):
    if filename.endswith(".json"):
        tables_count += 1
        filepath = os.path.join("data", filename)
        with open(filepath, 'r') as f:
            table_data = json.load(f)
            rows = len(table_data["data"])
            total_rows += rows
            print(f"   {filename}: {rows} rows")

print(f"\n   Total tables: {tables_count}")
print(f"   Total rows across all tables: {total_rows}")

print("\n" + "=" * 50)
print("Test complete!")
print(f"Database location: {os.path.abspath('data')}")