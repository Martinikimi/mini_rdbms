import json
import os

class Database:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        
        # Create folder if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    def create_table(self, table_name, columns):
        """Create a new table."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if os.path.exists(table_file):
            return f"Error: Table '{table_name}' already exists"
        
        table = {
            "name": table_name,
            "columns": columns,
            "data": []
        }
        
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        return f"Table '{table_name}' created"
    
    def insert(self, table_name, values):
        """Insert a row into a table."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        table["data"].append(values)
        
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        return f"Inserted into '{table_name}'"
    
    def select_all(self, table_name):
        """Get all rows from a table."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return []
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        return table["data"]