"""
Final Comprehensive CRUD Test
Tests: CREATE, INSERT, SELECT, UPDATE, DELETE
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.executor import SQLExecutor

def run_final_crud_test():
    print("FINAL COMPREHENSIVE CRUD TEST")
    print("=" * 60)
    
    # Use fresh test database
    test_dir = "test_final_crud"
    if os.path.exists(test_dir):
        import shutil
        shutil.rmtree(test_dir)
    
    executor = SQLExecutor(test_dir)
    
    test_results = {
        "create": False,
        "insert": False,
        "select": False,
        "update": False,
        "delete": False,
        "drop": False
    }
    
    print("\n1. CREATE TABLE:")
    result = executor.execute("CREATE TABLE employees (id, name, department, salary)")
    print(f"   Result: {result}")
    test_results["create"] = "created" in result.lower()
    
    print("\n2. INSERT multiple rows:")
    employees = [
        (1, 'Alice Johnson', 'Engineering', 75000),
        (2, 'Bob Smith', 'Marketing', 65000),
        (3, 'Charlie Brown', 'Engineering', 82000),
        (4, 'Diana Prince', 'HR', 55000),
        (5, 'Ethan Hunt', 'Engineering', 90000),
    ]
    
    insert_success = 0
    for emp in employees:
        result = executor.execute(f"INSERT INTO employees VALUES ({emp[0]}, '{emp[1]}', '{emp[2]}', {emp[3]})")
        print(f"   {emp[1]}: {result}")
        if "Inserted" in result or "inserted" in result:
            insert_success += 1
    
    test_results["insert"] = insert_success == len(employees)
    
    print("\n3. SELECT all (READ):")
    all_emps = executor.execute("SELECT * FROM employees")
    print(f"   Found {len(all_emps)} employees")
    if len(all_emps) == 5:
        print("   ✓ Correct count")
        test_results["select"] = True
    else:
        print(f"   ✗ Expected 5, got {len(all_emps)}")
    
    print("\n4. SELECT specific columns:")
    names_depts = executor.execute("SELECT name, department FROM employees")
    print(f"   Sample: {names_depts[0] if names_depts else 'None'}")
    if names_depts and len(names_depts[0]) == 2:
        print("   ✓ Correct column selection")
    
    print("\n5. UPDATE single row:")
    print("   Before UPDATE - Charlie's department:")
    charlie_before = executor.execute("SELECT * FROM employees WHERE name='Charlie Brown'")
    print(f"   - {charlie_before[0] if charlie_before else 'Not found'}")
    
    result = executor.execute("UPDATE employees SET department='Management' WHERE name='Charlie Brown'")
    print(f"   UPDATE result: {result}")
    
    print("   After UPDATE - Charlie's department:")
    charlie_after = executor.execute("SELECT * FROM employees WHERE name='Charlie Brown'")
    print(f"   - {charlie_after[0] if charlie_after else 'Not found'}")
    
    if "Updated 1 row" in result:
        print("   ✓ UPDATE successful")
        test_results["update"] = True
    
    print("\n6. UPDATE multiple rows:")
    result = executor.execute("UPDATE employees SET salary=80000 WHERE department='Engineering'")
    print(f"   UPDATE result: {result}")
    
    eng_emps = executor.execute("SELECT name, salary FROM employees WHERE department='Engineering'")
    print(f"   Engineering salaries: {eng_emps}")
    
    print("\n7. DELETE single row:")
    print("   Before DELETE count:", len(executor.execute("SELECT * FROM employees")))
    
    result = executor.execute("DELETE FROM employees WHERE name='Diana Prince'")
    print(f"   DELETE result: {result}")
    
    print("   After DELETE count:", len(executor.execute("SELECT * FROM employees")))
    
    if "Deleted 1 row" in result:
        print("   ✓ DELETE successful")
        test_results["delete"] = True
    
    print("\n8. DELETE multiple rows:")
    result = executor.execute("DELETE FROM employees WHERE department='Engineering'")
    print(f"   DELETE result: {result}")
    
    remaining = executor.execute("SELECT * FROM employees")
    print(f"   Remaining employees: {remaining}")
    
    print("\n9. DELETE all (clear table):")
    result = executor.execute("DELETE FROM employees")
    print(f"   Result: {result}")
    
    empty = executor.execute("SELECT * FROM employees")
    print(f"   Table empty: {len(empty) == 0}")
    
    print("\n10. DROP TABLE:")
    result = executor.execute("DROP TABLE employees")
    print(f"   Result: {result}")
    test_results["drop"] = "deleted" in result.lower() or "dropped" in result.lower()
    
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)
    
    for operation, passed in test_results.items():
        status = "✅ PASS" if passed else "❌ FAIL"
        print(f"{operation.upper():10} {status}")
    
    all_passed = all(test_results.values())
    if all_passed:
        print("\n🎉 ALL CRUD OPERATIONS WORKING PERFECTLY!")
    else:
        failed = [op for op, passed in test_results.items() if not passed]
        print(f"\n⚠️  Issues with: {', '.join(failed)}")
    
    # Cleanup
    if os.path.exists(test_dir):
        shutil.rmtree(test_dir)
    
    return all_passed

if __name__ == "__main__":
    run_final_crud_test()