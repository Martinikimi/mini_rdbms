
"""
Test indexing functionality
"""
import sys
import os
import shutil
import tempfile
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.database import Database

def test_indexing():
    print("Testing Database Indexing")
    print("=" * 60)
    
    # Use fresh test database
    temp_dir = tempfile.mkdtemp()
    db = Database(temp_dir)
    
    passed = 0
    failed = 0
    
    print("\n1. Creating table with PRIMARY KEY and UNIQUE:")
    column_defs = [
        {'name': 'id', 'type': 'INT', 'type_params': [], 'constraints': ['PRIMARY KEY']},
        {'name': 'name', 'type': 'VARCHAR(50)', 'type_params': [50], 'constraints': []},
        {'name': 'email', 'type': 'VARCHAR(100)', 'type_params': [100], 'constraints': ['UNIQUE']},
    ]
    
    result = db.create_table('users', column_defs)
    print(f"   {result}")
    if "created" in result.lower():
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n2. Inserting data:")
    test_data = [
        [1, 'Alice', 'alice@test.com'],
        [2, 'Bob', 'bob@test.com'],
        [3, 'Charlie', 'charlie@test.com'],
    ]
    
    for data in test_data:
        result = db.insert('users', data)
        print(f"   {data[1]}: {result}")
        if "Inserted" in result or "inserted" in result:
            passed += 1
        else:
            failed += 1
    
    print("\n3. Checking automatic index creation:")
    indexes = db.show_indexes('users')
    if indexes and len(indexes) >= 2:  # Should have id and email indexes
        print(f"   Found {len(indexes)} indexes: {list(indexes.keys())}")
        print("    PASS")
        passed += 1
    else:
        print(f"   Indexes: {indexes}")
        print("    FAIL - Expected 2+ indexes")
        failed += 1
    
    print("\n4. Testing SELECT with WHERE using index:")
    result = db.select_with_where('users', 'id=2')
    print(f"   SELECT * FROM users WHERE id=2: {result}")
    if result and len(result) == 1 and result[0][0] == 2:
        print("    PASS - Correctly found Bob")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n5. Creating manual index on name:")
    result = db.create_index('users', 'name')
    print(f"   {result}")
    if "created" in result.lower():
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n6. Testing index on name column:")
    result = db.select_with_where('users', 'name=\'Alice\'')
    print(f"   SELECT * FROM users WHERE name='Alice': {result}")
    if result and len(result) == 1 and result[0][1] == 'Alice':
        print("    PASS")
        passed += 1
    else:
        print("    FAIL")
        failed += 1
    
    print("\n7. Testing UPDATE maintains indexes:")
    # First check current indexes
    before_indexes = db.show_indexes('users')
    print(f"   Indexes before update: {list(before_indexes.keys()) if before_indexes else 'None'}")
    
    # Update a row
    result = db.update('users', {'name': 'Alice Updated'}, 'id=1')
    print(f"   UPDATE users SET name='Alice Updated' WHERE id=1: {result}")
    
    # Check indexes still exist
    after_indexes = db.show_indexes('users')
    print(f"   Indexes after update: {list(after_indexes.keys()) if after_indexes else 'None'}")
    
    if after_indexes and len(after_indexes) >= 2:
        print("    PASS - Indexes maintained after update")
        passed += 1
    else:
        print("    FAIL - Indexes lost after update")
        failed += 1
    
    print("\n8. Testing DELETE maintains indexes:")
    result = db.delete('users', 'id=3')
    print(f"   DELETE FROM users WHERE id=3: {result}")
    
    indexes_after_delete = db.show_indexes('users')
    if indexes_after_delete:
        print(f"   Indexes after delete: {list(indexes_after_delete.keys())}")
        print("    PASS - Indexes maintained after delete")
        passed += 1
    else:
        print("    FAIL - Indexes lost after delete")
        failed += 1
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")
    print(f"Total:  {passed + failed}")
    
    if failed == 0:
        print("\n All indexing tests passed!")
    else:
        print(f"\n  {failed} test(s) failed")
    
    # Cleanup
    shutil.rmtree(temp_dir)
    
    return passed, failed

if __name__ == "__main__":
    test_indexing()