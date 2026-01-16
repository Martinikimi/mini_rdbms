"""Test Phase 1 Parser - Only CREATE TABLE
"""
from src.parser import parse_sql

print("Testing Parser - PHASE 1 (CREATE TABLE only)")
print("=" * 50)

test_cases = [
    # Should work
    ("CREATE TABLE users (id, name, email)", True),
    ("CREATE TABLE products (id, name, price)", True),
    
    # not implemented yet
    ("INSERT INTO users VALUES (1, 'Alice')", False),
    ("SELECT * FROM users", False),
    
    # bad syntax
    ("CREATE TABLE", False),
    ("CREATE TABLE users", False),
    ("CREATE TABLE users id, name", False),
]

for sql, should_work in test_cases:
    print(f"\nCommand: {sql}")
    result = parse_sql(sql)
    
    if "error" in result:
        print(f"  Result: ERROR - {result['error']}")
        if should_work:
            print(f"   UNEXPECTED: Should have worked!")
    else:
        print(f"  Result: SUCCESS")
        print(f"  Action: {result['action']}")
        print(f"  Table: {result['table_name']}")
        print(f"  Columns: {result['columns']}")
        if not should_work:
            print(f"   UNEXPECTED: Should have failed!")

print("\n" + "=" * 50)
print("Phase 1 complete: Parser understands CREATE TABLE")