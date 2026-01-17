"""
Database Engine for mini_rdbms
Core database operations: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE
with Data Type Support and Basic Indexing
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
            "indexes": {},  # For indexing support
            "created_at": datetime.now().isoformat()
        }
        
        with open(table_file, 'w') as f:
            json.dump(table, f, indent=2)
        
        # Create indexes for PRIMARY KEY and UNIQUE columns
        self._create_indexes_for_table(table_name, column_defs)
        
        return f"Table '{table_name}' created with {len(column_defs)} columns"
    
    def _create_indexes_for_table(self, table_name, column_defs):
        """Create indexes for PRIMARY KEY and UNIQUE columns."""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return
        
        # Initialize indexes structure if not exists
        if "indexes" not in table_info:
            table_info["indexes"] = {}
        
        for i, col_def in enumerate(column_defs):
            if "PRIMARY KEY" in col_def.get("constraints", []) or "UNIQUE" in col_def.get("constraints", []):
                col_name = col_def["name"]
                self._create_index(table_name, col_name, i)
    
    def _create_index(self, table_name, column_name, column_index=None):
        """Create or rebuild an index on a column."""
        table_info = self.get_table_info(table_name)
        if not table_info:
            return
        
        if "indexes" not in table_info:
            table_info["indexes"] = {}
        
        # Get column index if not provided
        if column_index is None:
            column_defs = table_info.get("column_definitions", [])
            for i, col_def in enumerate(column_defs):
                if col_def["name"] == column_name:
                    column_index = i
                    break
        
        if column_index is None:
            return  # Column not found
        
        # Create hash index: value -> [row_indices]
        index_data = {}
        for row_idx, row in enumerate(table_info["data"]):
            value = row[column_index]
            if value not in index_data:
                index_data[value] = []
            index_data[value].append(row_idx)
        
        table_info["indexes"][column_name] = {
            "type": "hash",
            "data": index_data,
            "column_index": column_index
        }
        
        # Save updated table with index
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        with open(table_file, 'w') as f:
            json.dump(table_info, f, indent=2)
    
    def _update_index(self, table_name, column_name, operation, value, row_index):
        """Update an index after insert/update/delete."""
        table_info = self.get_table_info(table_name)
        if not table_info or "indexes" not in table_info or column_name not in table_info["indexes"]:
            return
        
        index = table_info["indexes"][column_name]
        
        if operation == "insert":
            if value not in index["data"]:
                index["data"][value] = []
            index["data"][value].append(row_index)
        
        elif operation == "update":
            # For simplicity, we'll rebuild the index on update
            # In a real system, we'd track old value and new value
            column_index = index.get("column_index")
            if column_index is not None:
                self._create_index(table_name, column_name, column_index)
        
        elif operation == "delete":
            # Remove this row from index
            if value in index["data"]:
                if row_index in index["data"][value]:
                    index["data"][value].remove(row_index)
                if not index["data"][value]:  # Empty list
                    del index["data"][value]
        
        # Save updated index
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        with open(table_file, 'w') as f:
            json.dump(table_info, f, indent=2)
    
    def _use_index_for_where(self, table_name, where_clause):
        """Check if WHERE clause can use an index and return matching row indices."""
        if not where_clause or '=' not in where_clause:
            return None
        
        parts = where_clause.split('=', 1)
        col_name = parts[0].strip()
        
        val = parts[1].strip()
        if (val.startswith("'") and val.endswith("'")) or \
           (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        
        table_info = self.get_table_info(table_name)
        if not table_info or "indexes" not in table_info or col_name not in table_info["indexes"]:
            return None
        
        index = table_info["indexes"][col_name]
        
        # SIMPLE FIX: Always look up as string
        # Convert index value to string for comparison
        for stored_val, row_indices in index["data"].items():
            if str(stored_val) == str(val):
                return row_indices
        
        return []  # Value not found
    
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
        new_row_index = len(table["data"])
        table["data"].append(typed_values)
        
        # Update indexes
        for i, col_def in enumerate(column_defs):
            if "PRIMARY KEY" in col_def.get("constraints", []) or "UNIQUE" in col_def.get("constraints", []):
                self._update_index(table_name, col_def["name"], "insert", typed_values[i], new_row_index)
        
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
    
    def select_with_where(self, table_name, where_clause=None):
        """Select rows with WHERE clause, using index if available."""
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        
        if not os.path.exists(table_file):
            return None
        
        with open(table_file, 'r') as f:
            table = json.load(f)
        
        # If no WHERE clause, return all rows
        if not where_clause:
            return table["data"]
        
        # Try to use index for WHERE clause
        indexed_rows = self._use_index_for_where(table_name, where_clause)
        if indexed_rows is not None:
            # Use index to get rows
            result = []
            for row_idx in indexed_rows:
                if row_idx < len(table["data"]):
                    result.append(table["data"][row_idx])
            return result
        
        # Fall back to full scan if no index
        return self._select_with_where_scan(table, where_clause)
    
    def _select_with_where_scan(self, table, where_clause):
        """Select rows by scanning all rows (fallback when no index)."""
        if not where_clause or '=' not in where_clause:
            return table["data"]
        
        # Parse WHERE clause
        parts = where_clause.split('=', 1)
        col_name = parts[0].strip()
        
        # Get value, removing quotes
        val = parts[1].strip()
        if (val.startswith("'") and val.endswith("'")) or \
           (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        
        # Get column index
        column_defs = table.get("column_definitions", [])
        col_index = None
        for i, col_def in enumerate(column_defs):
            if col_def["name"] == col_name:
                col_index = i
                break
        
        if col_index is None:
            # Column not found, return all rows?
            return table["data"]
        
        # Scan rows
        result = []
        for row in table["data"]:
            if str(row[col_index]) == str(val):
                result.append(row)
        
        return result
    
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
                    old_val = row[col_idx]
                    updated_row[col_idx] = new_val
                    
                    # Update index if this column is indexed
                    if "PRIMARY KEY" in col_def.get("constraints", []) or "UNIQUE" in col_def.get("constraints", []):
                        # For simplicity, rebuild index on update
                        # In a real system, we'd update more efficiently
                        self._update_index(table_name, col_def["name"], "update", new_val, row_idx)
                
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
            
            # Clear all indexes
            if "indexes" in table:
                for col_name in table["indexes"]:
                    table["indexes"][col_name]["data"] = {}
            
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
                
                for row_idx, row in enumerate(table["data"]):
                    if str(row[col_idx]) == str(val):
                        deleted_count += 1
                        # Update indexes for deleted row
                        for col_name, col_index in column_indices.items():
                            if "indexes" in table and col_name in table["indexes"]:
                                self._update_index(table_name, col_name, "delete", row[col_index], row_idx)
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
    
    def create_index(self, table_name, column_name):
        """
        Create an index on a column.
        
        Args:
            table_name: Name of table
            column_name: Name of column to index
            
        Returns:
            str: Success or error message
        """
        if not self.table_exists(table_name):
            return f"Error: Table '{table_name}' doesn't exist"
        
        column_defs = self.get_column_definitions(table_name)
        if not column_defs:
            return f"Error: Could not get column definitions for '{table_name}'"
        
        # Check if column exists
        col_exists = False
        col_index = None
        for i, col_def in enumerate(column_defs):
            if col_def["name"] == column_name:
                col_exists = True
                col_index = i
                break
        
        if not col_exists:
            return f"Error: Column '{column_name}' doesn't exist in table '{table_name}'"
        
        # Create the index
        self._create_index(table_name, column_name, col_index)
        
        return f"Index created on '{table_name}.{column_name}'"
    
    def show_indexes(self, table_name):
        """
        Show indexes for a table.
        
        Args:
            table_name: Name of table
            
        Returns:
            dict: Index information or None if table doesn't exist
        """
        table_info = self.get_table_info(table_name)
        if not table_info or "indexes" not in table_info:
            return None
        
        return table_info["indexes"]
    
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
            
            # Clear indexes
            if "indexes" in table:
                for col_name in table["indexes"]:
                    table["indexes"][col_name]["data"] = {}
            
            with open(table_file, 'w') as f:
                json.dump(table, f, indent=2)
            
            return f"Table '{table_name}' cleared (0 rows)"
            
        except Exception as e:
            return f"Error clearing table: {str(e)}"