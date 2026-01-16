"""
Database Engine for mini_rdbms
Core database operations: CREATE TABLE, INSERT, SELECT, DROP TABLE
"""
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
        # Validate table name
        if not table_name.isidentifier():
            return f"Error: Table name '{table_name}' is invalid. Table names must start with a letter or underscore and contain only letters, numbers, or underscores."
        
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
        
        # Validate column count
        expected_columns = len(table["columns"])
        received_values = len(values)
        
        if received_values != expected_columns:
            column_names = ", ".join(table["columns"])
            return f"Error: Table '{table_name}' has {expected_columns} columns ({column_names}), but {received_values} values were provided."
        
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
    
    def drop_table(self, table_name):
        """
        Delete a table from the database.
        
        Args:
            table_name: Name of table to delete
            
        Returns:
            str: Success or error message
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        try:
            # Delete the file
            os.remove(table_file)
            return f"Table '{table_name}' deleted successfully"
            
        except Exception as e:
            return f"Error deleting table: {str(e)}"
    
    def list_tables(self):
        """
        List all tables in the database.
        
        Returns:
            list: List of table names
        """
        tables = []
        for filename in os.listdir(self.data_dir):
            if filename.endswith(".json"):
                tables.append(filename[:-5])  # Remove ".json" extension
        return tables
    
    def table_exists(self, table_name):
        """
        Check if a table exists.
        
        Args:
            table_name: Name of table to check
            
        Returns:
            bool: True if table exists, False otherwise
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        return os.path.exists(table_file)
    
    def get_table_info(self, table_name):
        """
        Get information about a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            dict: Table information or None if table doesn't exist
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return None
        
        try:
            with open(table_file, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def count_rows(self, table_name):
        """
        Count rows in a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            int: Number of rows, or -1 if table doesn't exist
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return -1
        
        try:
            with open(table_file, 'r') as f:
                table = json.load(f)
                return len(table["data"])
        except:
            return -1
    
    def clear_table(self, table_name):
        """
        Clear all data from a table (keep structure).
        
        Args:
            table_name: Name of table to clear
            
        Returns:
            str: Success or error message
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        try:
            with open(table_file, 'r') as f:
                table = json.load(f)
            
            # Clear data but keep schema
            table["data"] = []
            
            with open(table_file, 'w') as f:
                json.dump(table, f, indent=2)
            
            return f"Table '{table_name}' cleared (0 rows)"
            
        except Exception as e:
            return f"Error clearing table: {str(e)}"