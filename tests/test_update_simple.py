import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from src.executor import SQLExecutor

def test_update_simple():
    print("SIMPLE UPDATE TEST")
    print("=" * 60)
    
    # Use fresh test database
    test_dir = "test_update_simple"
    if os.path.exists(test_dir):
        import shutil
        shutil.rmtree(test_dir)
    
    executor = SQLExecutor(test_dir)
    
    # 1. Create table
    print("\n1. CREATE TABLE:")
    result = executor.execute("CREATE TABLE users (id, name, age)")
    print(f"   {result}")
    
    # 2. Insert data
    print("\n2. INSERT data:")
    users = [
        (1, 'Alice', 25),
        (2, 'Bob', 30),
        (3, 'Charlie', 35),
    ]
    
    for user in users:
        result = executor.execute(f"INSERT INTO users VALUES ({user[0]}, '{user[1]}', {user[2]})")
        print(f"   {result}")
    
    # 3. Show initial data
    print("\n3. Initial data:")
    data = executor.execute("SELECT * FROM users")
    for row in data:
        print(f"   {row}")
    
    # 4. Test UPDATE with WHERE
    print("\n4. UPDATE Alice's age:")
    result = executor.execute("UPDATE users SET age=26 WHERE name='Alice'")
    print(f"   Result: {result}")
    
    print("   After UPDATE:")
    data = executor.execute("SELECT * FROM users")
    for row in data:
        print(f"   {row}")
    
    # 5. Test UPDATE without WHERE (should update all)
    print("\n5. UPDATE all ages (+1 year):")
    result = executor.execute("UPDATE users SET age=age+1")
    print(f"   Result: {result}")
    
    print("   After second UPDATE:")
    data = executor.execute("SELECT * FROM users")
    for row in data:
        print(f"   {row}")
    
    print("\n" + "=" * 60)
    print("UPDATE TEST COMPLETE")

if __name__ == "__main__":
    test_update_simple()