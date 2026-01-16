"""
Test SQL Parser - CREATE TABLE and INSERT commands
"""
from src.parser import parse_sql

print("Testing SQL Parser")
print("=" * 50)

test_cases = [
    ("CREATE TABLE users (id, name, email)", "create_table"),
    ("CREATE TABLE products (id, name, price, category)", "create_table"),
    ("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')", "insert"),
    ("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')", "insert"),
    ("INSERT INTO products VALUES (101, 'Laptop', 999.99, 'Electronics')", "insert"),
    ("INSERT INTO products VALUES (102, 'Mouse', 29.99, 'Accessories')", "insert"),
    ("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')", "insert"),
    ("INSERT INTO test VALUES ('string with, comma', 123, 'normal')", "insert"),
    ("SELECT * FROM users", "error"),
    ("DELETE FROM users", "error"),
    ("UPDATE users SET name = 'test'", "error"),
    ("INSERT INTO users", "error"),
    ("INSERT INTO users (1, 2, 3)", "error"),
    ("INSERT users VALUES (1, 2)", "error"),
]

passed = 0
failed = 0

for sql, expected_action in test_cases:
    print(f"\nCommand: {sql}")
    result = parse_sql(sql)
    
    if "error" in result:
        if expected_action == "error":
            print(f"  Result: Error (expected)")
            passed += 1
        else:
            print(f"  Result: Unexpected error - {result['error']}")
            failed += 1
    else:
        if result["action"] == expected_action:
            print(f"  Result: {result['action'].upper()} parsed")
            if result["action"] == "create_table":
                print(f"  Table: {result['table_name']}")
                print(f"  Columns: {result['columns']}")
            elif result["action"] == "insert":
                print(f"  Table: {result['table_name']}")
                print(f"  Values: {result['values']}")
            passed += 1
        else:
            print(f"  Result: Wrong action - got {result['action']}, expected {expected_action}")
            failed += 1

print("\n" + "=" * 50)
print(f"Summary: {passed} passed, {failed} failed")
print("=" * 50)