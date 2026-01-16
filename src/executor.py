"""
SQL Executor - Connects parser to database
"""
from src.database import Database
from src.parser import parse_sql

class SQLExecutor:
    def __init__(self, data_dir="data"):
        self.db = Database(data_dir)
    
    def execute(self, sql_command):
        """
        Execute a SQL command.
        
        Example:
            executor.execute("INSERT INTO users VALUES (1, 'Alice', 'test')")
            → Actually saves data to database
        """
        # 1. Parse the SQL
        parsed = parse_sql(sql_command)
        
        if "error" in parsed:
            return parsed["error"]
        
        # 2. Execute on database
        action = parsed["action"]
        
        if action == "create_table":
            return self.db.create_table(
                parsed["table_name"], 
                parsed["columns"]
            )
        
        elif action == "insert":
            return self.db.insert(
                parsed["table_name"],
                parsed["values"]
            )
        
        else:
            return f"Error: Action '{action}' not supported yet"