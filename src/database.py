"""
Database Engine for mini_rdbms
Core database operations: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE
with Data Type Support
"""
import json
import os
from datetime import datetime

class Database:
    def __init__(self, data_dir="data"):
        self.data_dir = data_dir
        
        # Create folder if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
    
    def create_table(self, table_name, columns):
        """Create a new table with column definitions."""
        # Validate table name
        if not table_name.isidentifier():
            return f"Error: Table name '{table_name}' is invalid. Table names must start with a letter or underscore and contain only letters, numbers, or underscores."
        
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if os.path.exists(table_file):
            return f"Error: Table '{table_name}' already exists"
        
        # Extract column names for backward compatibility
        column_names = []
        column_defs = []
        
        for col in columns:
            if isinstance(col, dict):
                # New style: column definition dict with type info
                column_names.append(col["name"])
                column_defs.append(col)
            else:
                # Old style: just column name (string)
                column_names.append(col)
                column_defs.append({
                    "name": col,
                    "type": "TEXT",
                    "type_params": [],
                    "constraints": []
                })
        
        table = {
            "name": table_name,
            "columns": column_names,  # For backward compatibility
            "column_definitions": column_defs,  # New: full column definitions
            "data": [],
            "indexes": {},  # For future indexing support
            "created_at": datetime.now().isoformat()
        }
        
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        return f"Table '{table_name}' created with {len(column_defs)} columns"
    
    def _validate_value_type(self, value, col_type, type_params):
        """Validate and convert a value to match the column type."""
        if value is None:
            return None, True  # NULL is allowed for all types unless NOT NULL constraint
        
        col_type_upper = col_type.upper()
        
        # Handle INT/INTEGER
        if col_type_upper in ["INT", "INTEGER"]:
            try:
                return int(value), True
            except (ValueError, TypeError):
                return f"Error: Value '{value}' cannot be converted to INT", False
        
        # Handle VARCHAR
        elif col_type_upper.startswith("VARCHAR"):
            if not isinstance(value, str):
                value = str(value)
            
            # Check length constraint if specified
            if type_params and len(type_params) > 0:
                max_length = type_params[0]
                if len(value) > max_length:
                    # Truncate with warning or return error
                    return value[:max_length], True  # Truncate for now
            return value, True
        
        # Handle DECIMAL
        elif col_type_upper.startswith("DECIMAL"):
            try:
                # Convert to float for now, could implement Decimal later
                float_val = float(value)
                
                # Handle precision/scale if specified
                if type_params and len(type_params) == 2:
                    precision, scale = type_params
                    # Simple precision/scale rounding
                    return round(float_val, scale), True
                return float_val, True
            except (ValueError, TypeError):
                return f"Error: Value '{value}' cannot be converted to DECIMAL", False
        
        # Handle FLOAT/REAL
        elif col_type_upper in ["FLOAT", "REAL"]:
            try:
                return float(value), True
            except (ValueError, TypeError):
                return f"Error: Value '{value}' cannot be converted to FLOAT", False
        
        # Handle BOOLEAN/BOOL
        elif col_type_upper in ["BOOLEAN", "BOOL"]:
            if isinstance(value, bool):
                return value, True
            elif isinstance(value, str):
                lower_val = value.lower()
                if lower_val in ["true", "1", "yes", "t"]:
                    return True, True
                elif lower_val in ["false", "0", "no", "f"]:
                    return False, True
                else:
                    return f"Error: Value '{value}' cannot be converted to BOOLEAN", False
            elif isinstance(value, (int, float)):
                return bool(value), True
            else:
                return f"Error: Value '{value}' cannot be converted to BOOLEAN", False
        
        # Handle DATE/DATETIME (simple string storage for now)
        elif col_type_upper in ["DATE", "DATETIME"]:
            # For now, store as string. Could add proper date parsing later
            return str(value), True
        
        # Handle TEXT/STRING (default)
        else:
            return str(value), True
    
    def _check_constraints(self, table, row_values, column_defs, exclude_row_index=None):
        """Check constraints for a row being inserted/updated."""
        # Build column index map
        col_index_map = {col_def["name"]: i for i, col_def in enumerate(column_defs)}
        
        # Check each column's constraints
        for i, col_def in enumerate(column_defs):
            value = row_values[i]
            constraints = col_def.get("constraints", [])
            
            # Check NOT NULL constraint
            if "NOT NULL" in constraints and value is None:
                return f"Error: Column '{col_def['name']}' cannot be NULL"
            
            # Check UNIQUE constraint (need to check against existing data)
            if "UNIQUE" in constraints or "PRIMARY KEY" in constraints:
                for row_idx, existing_row in enumerate(table["data"]):
                    if exclude_row_index is not None and row_idx == exclude_row_index:
                        continue  # Skip the row being updated
                    
                    if existing_row[i] == value:
                        return f"Error: Duplicate value '{value}' for unique column '{col_def['name']}'"
        
        return None  # All constraints satisfied
    
    def insert(self, table_name, values):
        """Insert a row into a table with type validation."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        # Get column definitions (new) or fall back to old format
        if "column_definitions" in table:
            column_defs = table["column_definitions"]
        else:
            # Convert old format to new format
            column_defs = []
            for col_name in table["columns"]:
                column_defs.append({
                    "name": col_name,
                    "type": "TEXT",
                    "type_params": [],
                    "constraints": []
                })
            table["column_definitions"] = column_defs
        
        # Validate column count
        expected_columns = len(column_defs)
        received_values = len(values)
        
        if received_values != expected_columns:
            column_names = ", ".join([col["name"] for col in column_defs])
            return f"Error: Table '{table_name}' has {expected_columns} columns ({column_names}), but {received_values} values were provided."
        
        # Validate and convert values
        typed_values = []
        for i, (value, col_def) in enumerate(zip(values, column_defs)):
            col_name = col_def["name"]
            validated_value, success = self._validate_value_type(
                value, 
                col_def["type"], 
                col_def.get("type_params", [])
            )
            
            if not success:
                return validated_value  # Returns error message
            
            typed_values.append(validated_value)
        
        # Check constraints
        constraint_error = self._check_constraints(table, typed_values, column_defs)
        if constraint_error:
            return constraint_error
        
        # Add the row
        table["data"].append(typed_values)
        
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
        """Update rows in a table with type validation."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return f"Error: Table '{table_name}' doesn't exist"
        
        # Load table data
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        # Get column definitions
        if "column_definitions" in table:
            column_defs = table["column_definitions"]
            columns = [col["name"] for col in column_defs]
        else:
            columns = table["columns"]
            # Convert to new format
            column_defs = []
            for col_name in columns:
                column_defs.append({
                    "name": col_name,
                    "type": "TEXT",
                    "type_params": [],
                    "constraints": []
                })
        
        column_indices = {col["name"]: idx for idx, col in enumerate(column_defs)}
        
        # Validate update columns exist and get type info
        update_info = []
        for col, new_val in updates.items():
            if col not in column_indices:
                return f"Error: Column '{col}' doesn't exist in table '{table_name}'"
            
            col_idx = column_indices[col]
            col_def = column_defs[col_idx]
            
            # Validate and convert the new value
            validated_value, success = self._validate_value_type(
                new_val,
                col_def["type"],
                col_def.get("type_params", [])
            )
            
            if not success:
                return validated_value  # Returns error message
            
            update_info.append((col_idx, col_def, validated_value))
        
        updated_count = 0
        
        # Update rows
        for row_idx, row in enumerate(table["data"]):
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
                # Create updated row copy
                updated_row = row.copy()
                
                # Apply updates
                for col_idx, col_def, new_val in update_info:
                    updated_row[col_idx] = new_val
                
                # Check constraints for the updated row
                constraint_error = self._check_constraints(
                    table, updated_row, column_defs, exclude_row_index=row_idx
                )
                
                if constraint_error:
                    return constraint_error  # Stop on first constraint violation
                
                # Apply the update
                table["data"][row_idx] = updated_row
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
                if "column_definitions" in table:
                    column_defs = table["column_definitions"]
                    columns = [col["name"] for col in column_defs]
                else:
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
    
    def get_column_definitions(self, table_name):
        """
        Get column definitions for a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            list: Column definitions or None if table doesn't exist
        """
        table_info = self.get_table_info(table_name)
        if not table_info:
            return None
        
        if "column_definitions" in table_info:
            return table_info["column_definitions"]
        else:
            # Convert old format
            column_defs = []
            for col_name in table_info["columns"]:
                column_defs.append({
                    "name": col_name,
                    "type": "TEXT",
                    "type_params": [],
                    "constraints": []
                })
            return column_defs
    
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