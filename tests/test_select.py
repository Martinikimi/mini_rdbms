"""
Simple SELECT Test - Query data from database
"""
import os
import sys

# Add src directory to path so we can import SQLExecutor
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.executor import SQLExecutor

def run_simple_select_test():
    print("SIMPLE SELECT TEST")
    print("=" * 60)
    
    # Create executor
    executor = SQLExecutor("data")
    
    # List of test queries
    test_queries = [
        ("SELECT * FROM users", "Get all users"),
        ("SELECT name, email FROM users", "Get names and emails"),
        ("SELECT * FROM products", "Get all products"),
        ("SELECT name, price FROM products", "Get product names and prices"),
        ("SELECT * FROM nonexistent", "Error: Table doesn't exist"),
        ("SELECT * FROM orders", "Empty table (if exists)"),
    ]
    
    passed = 0
    failed = 0
    
    print("Testing basic SELECT queries:")
    print("=" * 60)
    
    for sql, description in test_queries:
        print(f"\n{description}:")
        print(f"  Query: {sql}")
        result = executor.execute(sql)
        
        if "Error:" in str(result) or "error" in str(result).lower():
            if "nonexistent" in sql:
                print(f"  ✓ Got expected error: {result}")
                passed += 1
            else:
                print(f"  ✗ Unexpected error: {result}")
                failed += 1
        elif isinstance(result, list):
            print(f"  ✓ Success: {len(result)} rows")
            if result:
                print(f"    First row: {result[0]}")
            passed += 1
        else:
            print(f"  ✗ Unexpected result type: {type(result)} = {result}")
            failed += 1
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    
    if failed == 0:
        print("\n✅ SELECT is WORKING!")
        print("\nYou can now query your database:")
        print("  SELECT * FROM users")
        print("  SELECT name, email FROM users")
        print("  SELECT * FROM products")
        print("  SELECT name, price FROM products")
    else:
        print(f"\n⚠️  {failed} test(s) failed")
    
    # Additional: Test SELECT with specific columns
    print("\n" + "=" * 60)
    print("BONUS: Testing column selection")
    print("=" * 60)
    
    # Test column selection
    test_column_queries = [
        "SELECT name FROM users",
        "SELECT email FROM users",
        "SELECT id, name FROM users",
    ]
    
    for sql in test_column_queries:
        print(f"\n{sql}:")
        result = executor.execute(sql)
        if isinstance(result, list):
            print(f"  Result: {len(result)} rows")
            if result:
                print(f"  Sample: {result[0]}")
        else:
            print(f"  Result: {result}")

if __name__ == "__main__":
    run_simple_select_test()