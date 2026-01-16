"""
Test SQL Execution - Saves data to the main 'data' folder
"""
import os
import json
from src.executor import SQLExecutor

print("Testing SQL Execution - Data saved to 'data/' folder")
print("=" * 70)

# Use the main data folder
test_dir = "data"

# Create executor that uses the main data folder
executor = SQLExecutor(test_dir)  # Uses "data" as default anyway

test_cases = [
    "CREATE TABLE users (id, name, email, age)",
    "CREATE TABLE products (product_id, name, price, category)",
    "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com', 28)",
    "INSERT INTO users VALUES (2, 'Bob', 'bob@example.com', 35)",
    "INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com', 22)",
    "INSERT INTO products VALUES (101, 'Laptop', 999.99, 'Electronics')",
    "INSERT INTO products VALUES (102, 'Mouse', 29.99, 'Accessories')",
    "INSERT INTO products VALUES (103, 'Keyboard', 79.99, 'Accessories')",
]

print(f"Database folder: {test_dir}/")
print("=" * 70)

for i, sql in enumerate(test_cases, 1):
    print(f"\n{i}. Executing: {sql}")
    result = executor.execute(sql)
    print(f"   Result: {result}")

# Show what's in the data folder
print("\n" + "=" * 70)
print("CONTENTS OF 'data/' FOLDER (Your actual database):")
print("=" * 70)

if os.path.exists(test_dir):
    files = os.listdir(test_dir)
    if files:
        for file in files:
            if file.endswith('.json'):
                filepath = os.path.join(test_dir, file)
                size = os.path.getsize(filepath)
                print(f"\n{file} ({size} bytes):")
                
                with open(filepath, 'r') as f:
                    data = json.load(f)
                    columns = data['columns']
                    rows = data['data']
                    
                    print(f"  Table: {data['name']}")
                    print(f"  Columns: {', '.join(columns)}")
                    print(f"  Total Rows: {len(rows)}")
                    
                    for i, row in enumerate(rows[:5], 1):  
                        formatted = []
                        for j, value in enumerate(row):
                            col_name = columns[j] if j < len(columns) else f"col{j}"
                            formatted.append(f"{col_name}={value}")
                        print(f"    Row {i}: {', '.join(formatted)}")
                    
                    if len(rows) > 5:
                        print(f"    ... and {len(rows) - 5} more rows")
    else:
        print("No database files found")

print("\n" + "=" * 70)
print(" DATA SAVED TO YOUR MAIN DATABASE FOLDER!")
print("=" * 70)
print(f"\nYour data is now in: {os.path.abspath('data')}")
print("\nTo check your database anytime:")
print("1. Run: dir data")
print("2. View a table: cat data\\users.json")
print("3. Add more SQL: python this_script.py")
print("=" * 70)