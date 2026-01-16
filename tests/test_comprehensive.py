"""
Comprehensive test of mini_rdbms database engine
"""
import os
import shutil
from src.database import Database

print("COMPREHENSIVE DATABASE ENGINE TEST")
print("=" * 60)

# Clean start
if os.path.exists("test_data"):
    shutil.rmtree("test_data")

# Create fresh database
db = Database("test_data")

# TEST 1: Create table
print("\n1. Creating 'employees' table...")
result = db.create_table("employees", ["emp_id", "name", "department", "salary"])
print(f"   Result: {result}")

# TEST 2: Try duplicate table
print("\n2. Testing duplicate table prevention...")
result = db.create_table("employees", ["id", "name"])
print(f"   Result: {result}")

# TEST 3: Insert with correct columns
print("\n3. Inserting employees (correct column count)...")
employees = [
    [101, "John Doe", "Engineering", 75000],
    [102, "Jane Smith", "Marketing", 65000],
    [103, "Bob Wilson", "Sales", 55000]
]
for emp in employees:
    result = db.insert("employees", emp)
    print(f"   Inserted {emp[1]}: {result}")

# TEST 4: Insert with wrong column count
print("\n4. Testing insert with wrong column count...")
result = db.insert("employees", [104, "Alice Jones"])  # Missing 2 columns
print(f"   Result: {result}")

# TEST 5: Select from empty table
print("\n5. Selecting from empty 'projects' table...")
# First create empty table
db.create_table("projects", ["project_id", "name", "status"])
result = db.select_all("projects")
print(f"   Result: {result}")

# TEST 6: Select from non-existent table
print("\n6. Selecting from non-existent table...")
result = db.select_all("nonexistent")
print(f"   Result: {result}")

# TEST 7: Insert into non-existent table
print("\n7. Inserting into non-existent table...")
result = db.insert("ghost_table", [1, "test"])
print(f"   Result: {result}")

# TEST 8: Multiple tables independently
print("\n8. Testing multiple independent tables...")
db.create_table("departments", ["dept_id", "name", "budget"])
db.insert("departments", [1, "Engineering", 1000000])
db.insert("departments", [2, "Marketing", 500000])

employees_data = db.select_all("employees")
depts_data = db.select_all("departments")
projects_data = db.select_all("projects")

print(f"   Employees: {len(employees_data)} rows")
print(f"   Departments: {len(depts_data)} rows")
print(f"   Projects: {len(projects_data)} rows")

# TEST 9: Data persistence check
print("\n9. Checking data persistence...")
print("   Checking test_data/ folder:")
for file in os.listdir("test_data"):
    filepath = os.path.join("test_data", file)
    size = os.path.getsize(filepath)
    print(f"   - {file} ({size} bytes)")

# TEST 10: Verify JSON structure
print("\n10. Verifying JSON structure...")
import json
with open("test_data/employees.json", "r") as f:
    emp_data = json.load(f)
    print(f"   employees.json has:")
    print(f"   - {len(emp_data['columns'])} columns: {emp_data['columns']}")
    print(f"   - {len(emp_data['data'])} rows")

# TEST 11: Edge cases - table names
print("\n11. Testing edge cases...")
# Try invalid table name
result = db.create_table("123invalid", ["id", "name"])  # Starts with number
print(f"   Invalid table name '123invalid': {result}")

# TEST 12: Data integrity
print("\n12. Testing data integrity...")
# Recreate database instance to simulate restart
db2 = Database("test_data")
employees_reloaded = db2.select_all("employees")
print(f"   After 'restart', employees has: {len(employees_reloaded)} rows")
print(f"   First employee: {employees_reloaded[0] if employees_reloaded else 'None'}")

print("\n" + "=" * 60)
print("TEST SUMMARY")
print("=" * 60)

tables = [f for f in os.listdir("test_data") if f.endswith('.json')]
total_rows = 0
for table_file in tables:
    with open(os.path.join("test_data", table_file), 'r') as f:
        data = json.load(f)
        rows = len(data['data'])
        total_rows += rows
        print(f"   {table_file}: {rows} rows")

print(f"\n   Total tables: {len(tables)}")
print(f"   Total rows: {total_rows}")

# Clean up
shutil.rmtree("test_data")
print("\nCleaned up test_data/ folder")

print("\n" + "=" * 60)
print("COMPREHENSIVE TEST COMPLETE!")