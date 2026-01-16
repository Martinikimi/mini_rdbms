"""
SQL Parser for mini_rdbms - PHASE 1
"""
def parse_sql(sql_command):
    """
    Parse very basic SQL commands.
    PHASE 1: Only recognizes CREATE TABLE
    """
    sql = sql_command.strip()
    if not sql:
        return {"error": "Empty command"}
    
    sql_upper = sql.upper()
    
    # CREATE TABLE
    if sql_upper.startswith("CREATE TABLE"):
        parts = sql.split()
        if len(parts) < 4:
            return {"error": "Invalid CREATE TABLE syntax"}
        
        table_name = parts[2]
        
        # Find columns between parentheses
        start = sql.find('(')
        end = sql.find(')')
        
        if start == -1 or end == -1:
            return {"error": "Missing parentheses in CREATE TABLE"}
        
        columns_str = sql[start+1:end]
        columns = [col.strip() for col in columns_str.split(',') if col.strip()]
        
        return {
            "action": "create_table",
            "table_name": table_name,
            "columns": columns
        }
    
    else:
        return {"error": f"Command not supported yet: {sql}"}