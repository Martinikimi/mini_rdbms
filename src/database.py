"""
Database Engine for mini_rdbms
Core database operations: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE
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
            return None
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        return table["data"]
    
    def update(self, table_name, updates, where_clause=None):
        """Update rows in a table."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        # Load table data
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        # Get column indices
        columns = table["columns"]
        column_indices = {col: idx for idx, col in enumerate(columns)}
        
        # Check if update columns exist
        for col in updates.keys():
            if col not in columns:
                return f"Error: Column '{col}' doesn't exist in table '{table_name}'"
        
        updated_count = 0
        
        # Update rows
        for row in table["data"]:
            # Check WHERE condition
            should_update = True
            if where_clause:
                # Parse WHERE clause
                # Support: column='value' or column=value
                if '=' in where_clause:
                    # Split by = but be careful with quotes
                    parts = where_clause.split('=', 1)
                    col_name = parts[0].strip()
                    
                    # Get value, removing quotes
                    val = parts[1].strip()
                    if (val.startswith("'") and val.endswith("'")) or \
                       (val.startswith('"') and val.endswith('"')):
                        val = val[1:-1]
                    
                    if col_name in column_indices:
                        # Compare values
                        row_value = row[column_indices[col_name]]
                        if str(row_value) != str(val):
                            should_update = False
                    else:
                        return f"Error: Column '{col_name}' in WHERE clause doesn't exist"
                else:
                    return f"Error: Invalid WHERE clause. Use: column=value"
            
            if should_update:
                # Apply updates
                for col, new_val in updates.items():
                    idx = column_indices[col]
                    row[idx] = new_val
                updated_count += 1
        
        # Save updated table
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        return f"Updated {updated_count} row(s) in '{table_name}'"
    
    def delete(self, table_name, where_clause=None):
        """Delete rows from a table."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        # Load table data
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        # If no WHERE clause, delete all rows
        if where_clause is None:
            deleted_count = len(table["data"])
            table["data"] = []
        else:
            # Parse WHERE clause
            # Support: column='value' or column=value
            if '=' in where_clause:
                # Split by = but be careful with quotes
                parts = where_clause.split('=', 1)
                col_name = parts[0].strip()
                
                # Get value, removing quotes
                val = parts[1].strip()
                if (val.startswith("'") and val.endswith("'")) or \
                   (val.startswith('"') and val.endswith('"')):
                    val = val[1:-1]
                
                # Get column indices
                columns = table["columns"]
                column_indices = {col: idx for idx, col in enumerate(columns)}
                
                if col_name not in column_indices:
                    return f"Error: Column '{col_name}' in WHERE clause doesn't exist"
                
                col_idx = column_indices[col_name]
                
                # Filter rows to keep (opposite of WHERE condition)
                new_data = []
                deleted_count = 0
                
                for row in table["data"]:
                    if str(row[col_idx]) == str(val):
                        deleted_count += 1
                    else:
                        new_data.append(row)
                
                table["data"] = new_data
            else:
                return f"Error: Invalid WHERE clause. Use: column=value"
        
        # Save updated table
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        return f"Deleted {deleted_count} row(s) from '{table_name}'"
    
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