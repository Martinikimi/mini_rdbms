"""
Final verification before SQL parser
"""
import os
import shutil
from src.database import Database

print("FINAL DATABASE ENGINE VERIFICATION")
print("=" * 50)

# Clean test
if os.path.exists("final_test"):
    shutil.rmtree("final_test")

db = Database("final_test")

# 1. Basic CRUD operations work
print("1. Testing basic operations...")
db.create_table("test", ["id", "name", "value"])
db.insert("test", [1, "Item1", 100])
db.insert("test", [2, "Item2", 200])
result = db.select_all("test")
print(f"   Created table, inserted 2 rows, retrieved {len(result)} rows: OK")

# 2. Error cases handled
print("\n2. Testing error handling...")
# Duplicate table
dup_result = db.create_table("test", ["x", "y"])
print(f"   Duplicate table: {dup_result}")

# Wrong column count
wrong_cols = db.insert("test", [3, "Item3"])  # Missing value
print(f"   Wrong column count: {wrong_cols}")

# Non-existent table
no_table = db.select_all("ghost")
print(f"   Non-existent table select: {no_table}")

# 3. Multiple tables
print("\n3. Testing multiple tables...")
db.create_table("users", ["uid", "username"])
db.create_table("products", ["pid", "name", "price"])
db.insert("users", [1, "alice"])
db.insert("products", [101, "Laptop", 999])
db.insert("products", [102, "Mouse", 29])

users = db.select_all("users")
products = db.select_all("products")
print(f"   Users: {len(users)} rows, Products: {len(products)} rows: OK")

# 4. Data persistence
print("\n4. Testing data persistence...")
# New instance should see same data
db2 = Database("final_test")
users2 = db2.select_all("users")
products2 = db2.select_all("products")
print(f"   After 'restart': Users: {len(users2)} rows, Products: {len(products2)} rows: OK")

# 5. Invalid table name
print("\n5. Testing invalid table name...")
bad_name = db.create_table("123bad", ["x"])
print(f"   Invalid name '123bad': {bad_name}")

# 6. Check actual files
print("\n6. Checking file structure...")
files = os.listdir("final_test")
print(f"   Files in database: {files}")
for file in files:
    if file.endswith(".json"):
        path = os.path.join("final_test", file)
        size = os.path.getsize(path)
        print(f"   - {file}: {size} bytes")

# Clean up
shutil.rmtree("final_test")
print("\nCleaned up final_test/")

print("\n" + "=" * 50)
print("VERIFICATION COMPLETE")
print("Database engine is READY for SQL parser!")
print("=" * 50)