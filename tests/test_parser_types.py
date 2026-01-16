"""
Test parser with data types
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.parser import parse_sql

def test_parser_data_types():
    print("Testing Parser with Data Types")
    print("=" * 60)
    
    passed = 0
    failed = 0
    
    test_cases = [
        # Valid CREATE TABLE queries with data types (NEW STYLE)
        {
            "sql": "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT)",
            "should_succeed": True,
            "description": "Basic table with INT and VARCHAR"
        },
        {
            "sql": "CREATE TABLE products (id INT, name VARCHAR(100), price DECIMAL(10,2), in_stock BOOLEAN)",
            "should_succeed": True,
            "description": "Table with DECIMAL and BOOLEAN"
        },
        {
            "sql": "CREATE TABLE orders (order_id INT NOT NULL UNIQUE, customer_id INT, amount DECIMAL(8,2))",
            "should_succeed": True,
            "description": "Table with NOT NULL and UNIQUE constraints"
        },
        {
            "sql": "CREATE TABLE test (id INT, data TEXT, rating FLOAT, created DATE)",
            "should_succeed": True,
            "description": "Table with TEXT, FLOAT, and DATE"
        },
        
        # Valid CREATE TABLE queries (OLD STYLE - for backward compatibility)
        {
            "sql": "CREATE TABLE old_style (id, name, email)",
            "should_succeed": True,
            "description": "Old style without types (backward compatibility)"
        },
        
        # Invalid CREATE TABLE queries
        {
            "sql": "CREATE TABLE bad (id)",
            "should_succeed": False,
            "description": "Missing data type in new style"
        },
        {
            "sql": "CREATE TABLE bad2 (id UNKNOWN_TYPE)",
            "should_succeed": False,
            "description": "Unknown data type"
        },
        {
            "sql": "CREATE TABLE bad3 (id VARCHAR)",
            "should_succeed": False,
            "description": "VARCHAR without length"
        },
    ]
    
    for test in test_cases:
        print(f"\nTest: {test['description']}")
        print(f"SQL: {test['sql']}")
        
        result = parse_sql(test['sql'])
        
        if test['should_succeed']:
            if "error" in result:
                print(f" FAILED: Expected success, got error: {result['error']}")
                failed += 1
            else:
                print(f" PASSED")
                if result['action'] == 'create_table':
                    print(f"  Table: {result['table_name']}")
                    print(f"  Columns:")
                    for col in result['columns']:
                        constraints = ' '.join(col['constraints']) if col['constraints'] else ''
                        print(f"    - {col['name']}: {col['type']} {constraints}")
                passed += 1
        else:
            if "error" in result:
                print(f" PASSED: Got expected error: {result['error'][:50]}...")
                passed += 1
            else:
                print(f" FAILED: Expected error, but got success")
                failed += 1
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {passed + failed}")
    
    if failed == 0:
        print("\n All parser tests passed!")
    else:
        print(f"\n  {failed} test(s) failed")
    
    return passed, failed

if __name__ == "__main__":
    test_parser_data_types()