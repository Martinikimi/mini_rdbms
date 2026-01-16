"""
Test that actually EXECUTES SQL commands
"""
import os
import shutil
from src.executor import SQLExecutor

print("Testing SQL EXECUTION")
print("=" * 50)

# Clean test directory
test_dir = "execution_test"
if os.path.exists(test_dir):
    shutil.rmtree(test_dir)

# Create executor
executor = SQLExecutor(test_dir)

test_cases = [
    "CREATE TABLE users (id, name, email)",
    "CREATE TABLE products (id, name, price, category)",
    "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')",
    "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')",
    "INSERT INTO products VALUES (101, 'Laptop', 999.99, 'Electronics')",
]

print(f"Test database location: {test_dir}/")
print("=" * 50)

for i, sql in enumerate(test_cases, 1):
    print(f"\n{i}. Executing: {sql}")
    result = executor.execute(sql)
    print(f"   Result: {result}")

# Show what was actually created
print("\n" + "=" * 50)
print("Files created:")
print("=" * 50)

if os.path.exists(test_dir):
    files = os.listdir(test_dir)
    if files:
        for file in files:
            filepath = os.path.join(test_dir, file)
            size = os.path.getsize(filepath)
            print(f"  - {file} ({size} bytes)")
    else:
        print("  No files created")

# Clean up
shutil.rmtree(test_dir)
print(f"\nCleaned up {test_dir}/")
print("=" * 50)
print("Test complete")