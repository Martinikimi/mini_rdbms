"""
Test script for mini_rdbms database engine
"""
from src.database import Database

print("🧪 Testing mini_rdbms Database...")
print("=" * 40)

# 1. Create a database instance
print("1. Creating database instance...")
db = Database()
print("   ✓ Database created (using 'data/' folder)")

# 2. Create a table
print("\n2. Creating 'users' table...")
result = db.create_table("users", ["id", "name", "email"])
print(f"   Result: {result}")

# 3. Insert some users
print("\n3. Inserting users...")
users_to_insert = [
    [1, "Alice", "alice@example.com"],
    [2, "Bob", "bob@example.com"],
    [3, "Charlie", "charlie@example.com"]
]

for user in users_to_insert:
    result = db.insert("users", user)
    print(f"   Inserted {user[1]}: {result}")

# 4. Get all users
print("\n4. Retrieving all users...")
all_users = db.select_all("users")
print(f"   Found {len(all_users)} users:")

for i, user in enumerate(all_users, 1):
    print(f"   {i}. ID: {user[0]}, Name: {user[1]}, Email: {user[2]}")

# 5. Try to create duplicate table (should fail)
print("\n5. Testing error handling...")
result = db.create_table("users", ["id", "name"])
print(f"   Trying to create duplicate table: {result}")

print("\n" + "=" * 40)
print("✅ Test complete!")
print("Check 'data/' folder for users.json file")