"""
SQL Executor - Connects parser to database
Supports: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE, JOIN
"""
from src.database import Database
from src.parser import parse_sql
import os
import json

class SQLExecutor:
    def __init__(self, data_dir="data"):
        self.db = Database(data_dir)
        self.data_dir = data_dir
    
    def execute(self, sql_command):
        """
        Execute a SQL command.
        
        Supported:
        - CREATE TABLE table_name (col1, col2, ...)
        - INSERT INTO table_name VALUES (val1, val2, ...)
        - SELECT * FROM table_name [WHERE condition]
        - SELECT col1, col2 FROM table_name [WHERE condition]
        - SELECT * FROM table1 INNER JOIN table2 ON condition [WHERE condition]
        - UPDATE table_name SET column=value WHERE condition
        - DELETE FROM table_name WHERE condition
        - DROP TABLE table_name
        """
        # 1. Parse the SQL
        parsed = parse_sql(sql_command)
        
        if "error" in parsed:
            return parsed["error"]
        
        # 2. Execute based on action
        action = parsed["action"]
        
        if action == "create_table":
            return self.db.create_table(
                parsed["table_name"], 
                parsed["columns"]
            )
        
        elif action == "insert":
            # Check if table exists before inserting
            table_file = os.path.join(self.data_dir, f"{parsed['table_name']}.json")
            if not os.path.exists(table_file):
                return f"Error: Cannot insert into table '{parsed['table_name']}' - table doesn't exist"
            
            return self.db.insert(
                parsed["table_name"],
                parsed["values"]
            )
        
        elif action == "select_all":
            # Get rows from table with optional WHERE clause
            rows = self._execute_select(
                parsed["table_name"], 
                "*", 
                parsed.get("where")
            )
            if isinstance(rows, str) and "Error:" in rows:
                return rows
            return rows
        
        elif action == "select_columns":
            # Get rows with specific columns and optional WHERE clause
            rows = self._execute_select(
                parsed["table_name"], 
                parsed["columns"], 
                parsed.get("where")
            )
            if isinstance(rows, str) and "Error:" in rows:
                return rows
            return rows
        
        elif action == "select_join":
            # Execute JOIN query
            return self._execute_join(
                parsed["tables"],
                parsed["columns"],
                parsed["join_condition"],
                parsed.get("join_type", "INNER"),
                parsed.get("where")
            )
        
        elif action == "update":
            # Check if table exists
            table_file = os.path.join(self.data_dir, f"{parsed['table_name']}.json")
            if not os.path.exists(table_file):
                return f"Error: Table '{parsed['table_name']}' doesn't exist"
            
            return self.db.update(
                parsed["table_name"],
                parsed["updates"],
                parsed["where"]
            )
        
        elif action == "delete":
            # Check if table exists
            table_file = os.path.join(self.data_dir, f"{parsed['table_name']}.json")
            if not os.path.exists(table_file):
                return f"Error: Table '{parsed['table_name']}' doesn't exist"
            
            return self.db.delete(
                parsed["table_name"],
                parsed["where"]
            )
        
        elif action == "drop_table":
            return self.db.drop_table(parsed["table_name"])
        
        else:
            return f"Error: Action '{action}' not implemented"
    
    def _execute_select(self, table_name, columns, where_clause=None):
        """Execute a SELECT query with optional WHERE clause."""
        # Check if table exists
        if not self.db.table_exists(table_name):
            return f"Error: Table '{table_name}' doesn't exist"
        
        # Get rows (using index if WHERE clause and index exists)
        if where_clause:
            rows = self.db.select_with_where(table_name, where_clause)
        else:
            rows = self.db.select_all(table_name)
        
        if rows is None:
            return f"Error: Could not retrieve data from table '{table_name}'"
        
        # If selecting all columns or no filtering needed
        if columns == "*" or not rows:
            return rows
        
        # Filter to specific columns
        column_indices = []
        table_info = self._get_table_info(table_name)
        if table_info:
            # Get column indices for requested columns
            all_columns = table_info["columns"]
            for col in columns:
                if col in all_columns:
                    column_indices.append(all_columns.index(col))
                else:
                    return f"Error: Column '{col}' not found in table '{table_name}'"
        
        if not column_indices:
            return f"Error: No valid columns specified from table '{table_name}'"
        
        # Filter rows to only include requested columns
        filtered_rows = []
        for row in rows:
            filtered_row = [row[i] for i in column_indices]
            filtered_rows.append(filtered_row)
        
        return filtered_rows
    
    def _execute_join(self, tables, columns, join_condition, join_type="INNER", where_clause=None):
        """Execute a JOIN query."""
        if len(tables) != 2:
            return "Error: JOIN requires exactly 2 tables"
        
        table1, table2 = tables[0], tables[1]
        
        # Check if tables exist
        if not self.db.table_exists(table1):
            return f"Error: Table '{table1}' doesn't exist"
        
        if not self.db.table_exists(table2):
            return f"Error: Table '{table2}' doesn't exist"
        
        # Parse join condition: table.column = table.column
        # Example: customers.id = orders.customer_id
        if '=' not in join_condition:
            return "Error: Invalid JOIN condition. Use: table1.column = table2.column"
        
        left, right = join_condition.split('=', 1)
        left = left.strip()
        right = right.strip()
        
        # Extract table and column names
        left_parts = left.split('.')
        right_parts = right.split('.')
        
        if len(left_parts) != 2 or len(right_parts) != 2:
            return "Error: JOIN condition must be in format: table.column = table.column"
        
        left_table, left_col = left_parts[0].strip(), left_parts[1].strip()
        right_table, right_col = right_parts[0].strip(), right_parts[1].strip()
        
        # Verify table names in JOIN condition match the tables we're joining
        if left_table not in [table1, table2] or right_table not in [table1, table2]:
            return "Error: Table names in JOIN condition don't match joined tables"
        
        # Get data from both tables
        data1 = self.db.select_all(table1)
        data2 = self.db.select_all(table2)
        
        if data1 is None or data2 is None:
            return "Error: Could not retrieve data from one or more tables"
        
        # Get table info for column indices
        info1 = self.db.get_table_info(table1)
        info2 = self.db.get_table_info(table2)
        
        if not info1 or not info2:
            return "Error: Could not get table information"
        
        # Get column indices for join condition
        col1_idx = -1
        col2_idx = -1
        
        if left_table == table1 and left_col in info1["columns"]:
            col1_idx = info1["columns"].index(left_col)
        elif left_table == table2 and left_col in info2["columns"]:
            col1_idx = info2["columns"].index(left_col)
        
        if right_table == table1 and right_col in info1["columns"]:
            col2_idx = info1["columns"].index(right_col)
        elif right_table == table2 and right_col in info2["columns"]:
            col2_idx = info2["columns"].index(right_col)
        
        if col1_idx == -1 or col2_idx == -1:
            return f"Error: Column in JOIN condition not found"
        
        # Perform nested loop join (simplest implementation)
        result = []
        for row1 in data1:
            for row2 in data2:
                # Get values to compare
                val1 = row1[col1_idx] if left_table == table1 else row2[col1_idx]
                val2 = row1[col2_idx] if right_table == table1 else row2[col2_idx]
                
                # Compare values (use string comparison for now)
                if str(val1) == str(val2):
                    # Combine rows
                    combined_row = row1 + row2
                    result.append(combined_row)
        
        # Apply WHERE clause if specified
        if where_clause and result:
            result = self._apply_where_to_join(result, where_clause, table1, table2, info1, info2)
        
        # Filter columns if needed
        if columns != "*" and result:
            result = self._filter_join_columns(result, columns, table1, table2, info1, info2)
        
        return result
    
    def _apply_where_to_join(self, rows, where_clause, table1, table2, info1, info2):
        """Apply WHERE clause to JOIN results."""
        if '=' not in where_clause:
            return rows  # Simple WHERE support only
        
        parts = where_clause.split('=', 1)
        col_name = parts[0].strip()
        val = parts[1].strip()
        
        # Remove quotes if present
        if (val.startswith("'") and val.endswith("'")) or \
           (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        
        # Check if column is qualified with table name
        if '.' in col_name:
            table_part, col_part = col_name.split('.')
            table_part = table_part.strip()
            col_part = col_part.strip()
            
            if table_part == table1 and col_part in info1["columns"]:
                col_idx = info1["columns"].index(col_part)
                offset = 0  # First table's columns
            elif table_part == table2 and col_part in info2["columns"]:
                col_idx = info2["columns"].index(col_part)
                offset = len(info1["columns"])  # Second table's columns
            else:
                return rows  # Column not found
        else:
            # Try both tables
            col_idx = -1
            offset = 0
            
            if col_name in info1["columns"]:
                col_idx = info1["columns"].index(col_name)
                offset = 0
            elif col_name in info2["columns"]:
                col_idx = info2["columns"].index(col_name)
                offset = len(info1["columns"])
            else:
                return rows  # Column not found
        
        if col_idx == -1:
            return rows
        
        # Filter rows
        filtered_rows = []
        for row in rows:
            if str(row[col_idx + offset]) == str(val):
                filtered_rows.append(row)
        
        return filtered_rows
    
    def _filter_join_columns(self, rows, columns, table1, table2, info1, info2):
        """Filter JOIN results to specific columns."""
        # Build map of column positions
        column_positions = {}
        
        # Add columns from first table
        for i, col_name in enumerate(info1["columns"]):
            column_positions[f"{table1}.{col_name}"] = i
            column_positions[col_name] = i  # Unqualified name
        
        # Add columns from second table (with offset)
        offset = len(info1["columns"])
        for i, col_name in enumerate(info2["columns"]):
            column_positions[f"{table2}.{col_name}"] = i + offset
            if col_name not in column_positions:  # Only add unqualified if not already present
                column_positions[col_name] = i + offset
        
        # Get column indices for requested columns
        column_indices = []
        for col in columns:
            if col in column_positions:
                column_indices.append(column_positions[col])
            else:
                # Column not found
                continue
        
        if not column_indices:
            return rows  # No valid columns specified
        
        # Filter rows
        filtered_rows = []
        for row in rows:
            filtered_row = [row[i] for i in column_indices]
            filtered_rows.append(filtered_row)
        
        return filtered_rows
    
    def _get_table_info(self, table_name):
        """
        Helper to get table column information.
        Used for SELECT column filtering.
        """
        table_file = os.path.join(self.data_dir, f"{table_name}.json")
        if not os.path.exists(table_file):
            return None
        
        try:
            with open(table_file, 'r') as f:
                return json.load(f)
        except:
            return None
    
    def format_result(self, result):
        """
        Format execution result for display.
        """
        if isinstance(result, list):
            if not result:
                return "No rows found"
            
            output = []
            for i, row in enumerate(result, 1):
                output.append(f"{i}. {row}")
            return "\n".join(output)
        
        return str(result)