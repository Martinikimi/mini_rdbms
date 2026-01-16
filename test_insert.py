"""
Test INSERT functionality - Uses existing tables from CREATE TABLE test
"""
import os
import json
from src.executor import SQLExecutor

print("Testing INSERT functionality")
print("=" * 60)

# Check if tables exist from previous test
print("Checking existing tables in data/ folder...")
if not os.path.exists("data"):
    print("ERROR: data/ folder doesn't exist. Run CREATE TABLE test first!")
    exit()

tables = [f for f in os.listdir("data") if f.endswith('.json')]
if not tables:
    print("ERROR: No tables found. Run CREATE TABLE test first!")
    exit()

print(f"Found {len(tables)} tables: {', '.join(tables)}")
print("=" * 60)

# Create executor using main data folder
executor = SQLExecutor("data")

# Test cases - using tables we created
test_cases = [
    # Valid INSERT commands
    ("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')", True),
    ("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')", True),
    ("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')", True),
    
    ("INSERT INTO products VALUES (101, 'Laptop', 999.99, 'Electronics')", True),
    ("INSERT INTO products VALUES (102, 'Mouse', 29.99, 'Accessories')", True),
    ("INSERT INTO products VALUES (103, 'Keyboard', 79.99, 'Accessories')", True),
    
    ("INSERT INTO orders VALUES (1001, 1, 101, 1)", True),
    ("INSERT INTO orders VALUES (1002, 2, 102, 2)", True),
    ("INSERT INTO orders VALUES (1003, 3, 103, 1)", True),
    
    # Invalid - wrong column count (users needs 3 columns, got 2)
    ("INSERT INTO users VALUES (4, 'Dave')", False),
    
    # Invalid - wrong column count (users needs 3 columns, got 4)
    ("INSERT INTO users VALUES (5, 'Eve', 'eve@test.com', 'extra')", False),
    
    # Invalid - non-existent table
    ("INSERT INTO ghost VALUES (1, 2, 3)", False),
    
    # With quotes and special characters
    ("INSERT INTO users VALUES (6, 'Frank O\\'Connor', 'frank@test.com')", True),
]

print(f"Test database: data/")
print("=" * 60)

passed = 0
total = len(test_cases)

for sql, should_succeed in test_cases:
    print(f"\nExecuting: {sql}")
    result = executor.execute(sql)
    
    if should_succeed:
        if "Inserted into" in result:
            print(f"  ✓ SUCCESS: {result}")
            passed += 1
        else:
            print(f"  ✗ FAILED: Expected success, got: {result}")
    else:
        if "Error:" in result or "error" in result.lower():
            print(f"  ✓ Expected error: {result}")
            passed += 1
        else:
            print(f"  ✗ FAILED: Expected error, got: {result}")

# Show database state after inserts
print("\n" + "=" * 60)
print("DATABASE STATE AFTER INSERTS:")
print("=" * 60)

total_rows = 0
for table_file in tables:
    filepath = os.path.join("data", table_file)
    with open(filepath, 'r') as f:
        data = json.load(f)
        rows = len(data['data'])
        total_rows += rows
        print(f"\n{table_file}:")
        print(f"  Table: {data['name']}")
        print(f"  Columns: {', '.join(data['columns'])}")
        print(f"  Rows: {rows}")
        
        if rows > 0 and rows <= 5:
            for i, row in enumerate(data['data'], 1):
                print(f"    Row {i}: {row}")
        elif rows > 5:
            for i, row in enumerate(data['data'][:3], 1):
                print(f"    Row {i}: {row}")
            print(f"    ... and {rows - 3} more rows")

print("\n" + "=" * 60)
print(f"TOTAL: {total_rows} rows across {len(tables)} tables")
print(f"TEST RESULTS: {passed}/{total} tests passed")
print("=" * 60)

# Show how to query the data
print("\n" + "=" * 60)
print("NEXT: Test SELECT to query this data!")
print("=" * 60)
print("\nYour database now has real data!")
print("Run these to see:")
print("  python -c \"from src.executor import SQLExecutor; e=SQLExecutor('data'); print(e.execute('SELECT * FROM users'))\"")
print("\nOr wait for SELECT test next!")