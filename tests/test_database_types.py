"""
Test database engine with data type support
"""
import sys
import os
import shutil
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database

def test_database_data_types():
    print("Testing Database Data Type Support")
    print("=" * 60)
    
    # Use fresh test database
    test_dir = "test_db_types"
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    db = Database(test_dir)
    
    passed = 0
    failed = 0
    
    print("\n1. Creating table with data types:")
    column_defs = [
        {'name': 'id', 'type': 'INT', 'type_params': [], 'constraints': ['PRIMARY KEY']},
        {'name': 'name', 'type': 'VARCHAR(50)', 'type_params': [50], 'constraints': ['NOT NULL']},
        {'name': 'age', 'type': 'INT', 'type_params': [], 'constraints': []},
        {'name': 'salary', 'type': 'DECIMAL(10,2)', 'type_params': [10, 2], 'constraints': []},
        {'name': 'active', 'type': 'BOOLEAN', 'type_params': [], 'constraints': []},
        {'name': 'notes', 'type': 'TEXT', 'type_params': [], 'constraints': []}
    ]
    
    result = db.create_table('employees', column_defs)
    print(f"   {result}")
    if "created" in result.lower():
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n2. Insert valid data:")
    test_cases = [
        ([1, 'Alice', 30, 50000.50, True, 'Manager'], "Valid row 1"),
        ([2, 'Bob', 25, 45000.00, False, 'Developer'], "Valid row 2"),
        ([3, 'Charlie', 35, 60000.75, True, 'Senior Dev'], "Valid row 3"),
    ]
    
    for values, description in test_cases:
        result = db.insert('employees', values)
        print(f"   {description}: {result}")
        if "Inserted" in result or "inserted" in result:
            print("    PASS")
            passed += 1
        else:
            print("    FAIL")
            failed += 1
    
    print("\n3. Test type validation errors:")
    error_cases = [
        (['not_a_number', 'Test', 30, 50000, True, 'test'], "String for INT column"),
        ([4, 'A' * 60, 30, 50000, True, 'test'], "VARCHAR too long (should truncate)"),
        ([5, 'Test', 'not_a_number', 50000, True, 'test'], "String for INT age"),
        ([6, 'Test', 30, 'not_a_number', True, 'test'], "String for DECIMAL"),
    ]
    
    for values, description in error_cases:
        result = db.insert('employees', values)
        print(f"   {description}: {result}")
        if "Error:" in result:
            print("    PASS (correctly rejected)")
            passed += 1
        else:
            print("    FAIL (should have rejected)")
            failed += 1
    
    print("\n4. Test NOT NULL constraint:")
    result = db.insert('employees', [7, None, 30, 50000, True, 'test'])
    print(f"   Insert NULL into NOT NULL column: {result}")
    if "Error:" in result and "cannot be NULL" in result:
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n5. Test UNIQUE/PRIMARY KEY constraint:")
    result = db.insert('employees', [1, 'Duplicate', 40, 70000, False, 'test'])
    print(f"   Duplicate primary key (id=1): {result}")
    if "Error:" in result and ("Duplicate" in result or "unique" in result.lower()):
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n6. Test UPDATE with type validation:")
    # First insert a row to update
    db.insert('employees', [10, 'UpdateTest', 40, 50000, True, 'test'])
    
    # Try to update with wrong type
    result = db.update('employees', {'age': 'not_a_number'}, 'id=10')
    print(f"   Update age with string: {result}")
    if "Error:" in result:
        print("    PASS (correctly rejected)")
        passed += 1
    else:
        print("    FAIL (should have rejected)")
        failed += 1
    
    # Try valid update
    result = db.update('employees', {'age': 45, 'salary': 55000.75}, 'id=10')
    print(f"   Valid update: {result}")
    if "Updated" in result:
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n7. Test SELECT to verify data:")
    data = db.select_all('employees')
    print(f"   Total rows: {len(data)}")
    if len(data) >= 3:  # Should have at least the 3 valid rows
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n8. Test backward compatibility (old style tables):")
    # Create old style table (just column names)
    result = db.create_table('old_style', ['id', 'name', 'email'])
    print(f"   Create old style table: {result}")
    if "created" in result.lower():
        print("   ✅ PASS")
        passed += 1
        
        # Insert into old style table
        result = db.insert('old_style', [1, 'Alice', 'alice@test.com'])
        print(f"   Insert into old style: {result}")
        if "Inserted" in result:
            print("    PASS")
            passed += 1
        else:
            print("    FAIL")
            failed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {passed + failed}")
    
    if failed == 0:
        print("\n All database data type tests passed!")
    else:
        print(f"\n  {failed} test(s) failed")
    
    # Cleanup
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    return passed, failed

if __name__ == "__main__":
    test_database_data_types()