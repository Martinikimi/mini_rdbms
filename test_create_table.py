"""
Test CREATE TABLE functionality - Uses main data/ folder
"""
import os
import json
from src.executor import SQLExecutor

print("Testing CREATE TABLE functionality")
print("=" * 60)

# Backup existing data folder
backup_dir = None
if os.path.exists("data"):
    import shutil
    import datetime
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_dir = f"data_backup_{timestamp}"
    shutil.copytree("data", backup_dir)
    print(f"Backed up existing data/ to {backup_dir}/")
    
    # Clear data folder for clean test
    shutil.rmtree("data")
    os.makedirs("data")

# Create executor using main data folder
executor = SQLExecutor("data")

test_cases = [
    # Valid CREATE TABLE commands
    ("CREATE TABLE users (id, name, email)", True, "users"),
    ("CREATE TABLE products (id, name, price, category)", True, "products"),
    ("CREATE TABLE orders (order_id, user_id, total)", True, "orders"),
    
    # Invalid - duplicate table
    ("CREATE TABLE users (x, y, z)", False, None),
    
    # Invalid - table name starting with number
    ("CREATE TABLE 123bad (id, name)", False, None),
    
    # Invalid - missing parentheses
    ("CREATE TABLE bad1 id, name", False, None),
]

print(f"Testing in main database folder: data/")
print("=" * 60)

passed = 0
total = len(test_cases)

for sql, should_succeed, expected_table in test_cases:
    print(f"\nExecuting: {sql}")
    result = executor.execute(sql)
    
    if should_succeed:
        if "Table '" in result and "created" in result:
            print(f"  ✓ SUCCESS: {result}")
            
            # Verify file was created in data/ folder
            table_file = os.path.join("data", f"{expected_table}.json")
            if os.path.exists(table_file):
                print(f"  ✓ File created: data/{expected_table}.json")
                
                # Verify JSON structure
                with open(table_file, 'r') as f:
                    table_data = json.load(f)
                    if table_data["name"] == expected_table:
                        print(f"  ✓ Correct table name in JSON")
                    else:
                        print(f"  ✗ Wrong table name in JSON: {table_data['name']}")
                
                passed += 1
            else:
                print(f"  ✗ ERROR: File not created in data/ folder!")
        else:
            print(f"  ✗ FAILED: Expected success, got: {result}")
    else:
        if "Error:" in result:
            print(f"  ✓ Expected error: {result}")
            passed += 1
        else:
            print(f"  ✗ FAILED: Expected error, got: {result}")

# Show what's in the data/ folder
print("\n" + "=" * 60)
print("CURRENT CONTENTS OF data/ FOLDER:")
print("=" * 60)

if os.path.exists("data"):
    files = os.listdir("data")
    if files:
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join("data", file)
                size = os.path.getsize(filepath)
                print(f"  - {file} ({size} bytes)")
                
                # Show basic info
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    print(f"    Table: {data['name']}, Columns: {len(data['columns'])}, Rows: {len(data['data'])}")
    else:
        print("  No files in data/ folder")

print("\n" + "=" * 60)
print(f"RESULTS: {passed}/{total} tests passed")
print("=" * 60)

# Restore backup if we had one
if backup_dir:
    print(f"\nNote: Original data/ was backed up to {backup_dir}/")
    print("Current test data is in data/ folder")
else:
    print(f"\nTest data is now in your main data/ folder")

print("\nTo check your database:")
print("  dir data")
print("  cat data\\users.json")