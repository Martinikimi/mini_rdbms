import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from src.executor import SQLExecutor

def test_join():
    print("Testing JOIN Operations")
    print("=" * 50)
    
    executor = SQLExecutor()
    
    # Clean up old test data
    for table in ['customers', 'orders']:
        if os.path.exists(f"data/{table}.json"):
            os.remove(f"data/{table}.json")
    
    # Create tables (SINGLE LINE!)
    print("1. Creating tables...")
    print(executor.execute("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))"))
    
    print(executor.execute("CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, amount DECIMAL(10,2))"))
    
    # Insert data
    print("\n2. Inserting data...")
    print(executor.execute("INSERT INTO customers VALUES (1, 'Alice')"))
    print(executor.execute("INSERT INTO customers VALUES (2, 'Bob')"))
    print(executor.execute("INSERT INTO customers VALUES (3, 'Charlie')"))
    
    print(executor.execute("INSERT INTO orders VALUES (101, 1, 99.99)"))
    print(executor.execute("INSERT INTO orders VALUES (102, 1, 49.99)"))
    print(executor.execute("INSERT INTO orders VALUES (103, 2, 149.99)"))
    
    # Test simple SELECT first
    print("\n3. Testing basic SELECT...")
    customers = executor.execute("SELECT * FROM customers")
    print("Customers:", customers)
    
    orders = executor.execute("SELECT * FROM orders")
    print("Orders:", orders)
    
    # Test JOIN
    print("\n4. Testing INNER JOIN...")
    result = executor.execute(
        "SELECT * FROM customers INNER JOIN orders ON customers.id = orders.customer_id"
    )
    print("JOIN Result:", result)
    print("Rows found:", len(result) if isinstance(result, list) else "N/A")
    
    if isinstance(result, list):
        print(f"Expected 3 rows, got {len(result)} rows")
        for i, row in enumerate(result, 1):
            print(f"  Row {i}: {row}")
    
    # Test JOIN with WHERE
    print("\n5. Testing JOIN with WHERE...")
    result = executor.execute(
        "SELECT customers.name, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE orders.amount > 50"
    )
    print("JOIN with WHERE Result:", result)
    
    # Test specific columns
    print("\n6. Testing JOIN with specific columns...")
    result = executor.execute(
        "SELECT customers.name, orders.order_id, orders.amount FROM customers INNER JOIN orders ON customers.id = orders.customer_id"
    )
    print("Specific columns Result:", result)
    
    # Test JOIN with qualified column names
    print("\n7. Testing JOIN with qualified column names...")
    result = executor.execute(
        "SELECT customers.name, orders.order_id FROM customers INNER JOIN orders ON customers.id = orders.customer_id WHERE orders.customer_id = 1"
    )
    print("Qualified columns Result:", result)
    
    print("\n" + "=" * 50)
    print("JOIN Test Complete")
    
    # Clean up
    for table in ['customers', 'orders']:
        if os.path.exists(f"data/{table}.json"):
            os.remove(f"data/{table}.json")

if __name__ == "__main__":
    test_join()