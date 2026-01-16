"""
SQL Executor - Connects parser to database
Supports: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE
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
        - SELECT * FROM table_name
        - SELECT col1, col2 FROM table_name
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
            # Get all rows from table
            rows = self.db.select_all(parsed["table_name"])
            if rows is None:
                return f"Error: Table '{parsed['table_name']}' doesn't exist"
            return rows
        
        elif action == "select_columns":
            # Check if table exists first
            table_file = os.path.join(self.data_dir, f"{parsed['table_name']}.json")
            if not os.path.exists(table_file):
                return f"Error: Table '{parsed['table_name']}' doesn't exist"
            
            # Get all rows from table
            rows = self.db.select_all(parsed["table_name"])
            
            # Basic column filtering
            if parsed["columns"] != ["*"]:
                # Simple column filtering
                column_indices = []
                table_info = self._get_table_info(parsed["table_name"])
                if table_info:
                    for col in parsed["columns"]:
                        if col in table_info["columns"]:
                            column_indices.append(table_info["columns"].index(col))
                
                if column_indices:
                    filtered_rows = []
                    for row in rows:
                        filtered_row = [row[i] for i in column_indices]
                        filtered_rows.append(filtered_row)
                    return filtered_rows
                else:
                    # No valid columns specified
                    return f"Error: Column(s) {parsed['columns']} not found in table '{parsed['table_name']}'"
            
            return rows
        
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
            # Check if drop_table method exists
            if hasattr(self.db, 'drop_table'):
                return self.db.drop_table(parsed["table_name"])
            else:
                return "Error: DROP TABLE not implemented in database engine"
        
        else:
            return f"Error: Action '{action}' not implemented"
    
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