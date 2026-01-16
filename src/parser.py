"""
SQL Parser for mini_rdbms -
CREATE TABLE and INSERT INTO
"""
def parse_sql(sql_command):
    """
    Parse basic SQL commands.
    PHASE 2: Supports CREATE TABLE and INSERT INTO
    """
    sql = sql_command.strip()
    if not sql:
        return {"error": "Empty command"}
    
    sql_upper = sql.upper()
    
    # CREATE TABLE command
    if sql_upper.startswith("CREATE TABLE"):
        return _parse_create_table(sql)
    
    # INSERT INTO command
    elif sql_upper.startswith("INSERT INTO"):
        return _parse_insert(sql)
    
    else:
        return {"error": f"Command not supported yet: {sql}"}


def _parse_create_table(sql):
    """
    Parse: CREATE TABLE table_name (col1, col2, ...)
    """
    # Simple parsing
    parts = sql.split()
    if len(parts) < 4:
        return {"error": "Invalid CREATE TABLE syntax. Use: CREATE TABLE name (col1, col2, ...)"}
    
    table_name = parts[2]
    
    # Find columns between parentheses
    start = sql.find('(')
    end = sql.find(')')
    
    if start == -1 or end == -1:
        return {"error": "Missing parentheses in CREATE TABLE"}
    
    columns_str = sql[start+1:end]
    columns = [col.strip() for col in columns_str.split(',') if col.strip()]
    
    if not columns:
        return {"error": "No columns specified"}
    
    return {
        "action": "create_table",
        "table_name": table_name,
        "columns": columns
    }


def _parse_insert(sql):
    """
    Parse: INSERT INTO table_name VALUES (val1, val2, ...)
    PHASE 2: New function
    """
    parts = sql.split()
    if len(parts) < 5 or parts[3].upper() != "VALUES":
        return {"error": "Invalid INSERT syntax. Use: INSERT INTO table VALUES (...)"}
    
    table_name = parts[2]
    
    # Find values between parentheses
    start = sql.find('(')
    end = sql.find(')')
    
    if start == -1 or end == -1:
        return {"error": "Missing parentheses in INSERT"}
    
    values_str = sql[start+1:end]
    
    # Split by comma, but handle quoted strings
    values = []
    current = ""
    in_quotes = False
    
    for char in values_str:
        if char == "'" and not in_quotes:
            in_quotes = True
            current += char
        elif char == "'" and in_quotes:
            in_quotes = False
            current += char
        elif char == ',' and not in_quotes:
            values.append(current.strip())
            current = ""
        else:
            current += char
    
    if current:
        values.append(current.strip())
    
    # Clean and convert values
    cleaned_values = []
    for val in values:
        if not val:
            continue
        
        # Remove quotes if string
        if val.startswith("'") and val.endswith("'"):
            val = val[1:-1]
        
        # Try to convert to number
        try:
            if '.' in val:
                cleaned_values.append(float(val))
            else:
                cleaned_values.append(int(val))
        except ValueError:
            cleaned_values.append(val)
    
    return {
        "action": "insert",
        "table_name": table_name,
        "values": cleaned_values
    }