"""
SQL Parser for mini_rdbms
Supports: CREATE TABLE, INSERT, SELECT, DROP TABLE
"""
import re

def parse_sql(sql_command):
    """
    Parse SQL commands.
    Supports: CREATE TABLE, INSERT, SELECT, DROP TABLE
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
    
    # SELECT command
    elif sql_upper.startswith("SELECT"):
        return _parse_select(sql)
    
    # DROP TABLE command
    elif sql_upper.startswith("DROP TABLE"):
        return _parse_drop_table(sql)
    
    else:
        return {"error": f"Command not supported: {sql}"}


def _parse_create_table(sql):
    """
    Parse: CREATE TABLE table_name (col1, col2, ...)
    """
    # Use regex for better parsing
    pattern = r"CREATE TABLE (\w+)\s*\((.*)\)"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid CREATE TABLE syntax. Use: CREATE TABLE name (col1, col2, ...)"}
    
    table_name = match.group(1)
    columns_str = match.group(2)
    
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
    """
    pattern = r"INSERT INTO (\w+)\s+VALUES\s*\((.*)\)"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid INSERT syntax. Use: INSERT INTO table VALUES (...)"}
    
    table_name = match.group(1)
    values_str = match.group(2)
    
    values = _parse_values(values_str)
    
    if values is None:
        return {"error": f"Could not parse values: {values_str}"}
    
    return {
        "action": "insert",
        "table_name": table_name,
        "values": values
    }


def _parse_select(sql):
    """
    Parse: SELECT * FROM table_name
    Also handles: SELECT column1, column2 FROM table_name (basic)
    """
    # Simple SELECT * FROM table
    pattern_simple = r"SELECT \*\s+FROM\s+(\w+)"
    match = re.match(pattern_simple, sql, re.IGNORECASE)
    
    if match:
        return {
            "action": "select_all",
            "table_name": match.group(1),
            "columns": "*"
        }
    
    # SELECT specific columns
    pattern_cols = r"SELECT (.+)\s+FROM\s+(\w+)"
    match = re.match(pattern_cols, sql, re.IGNORECASE)
    
    if match:
        columns_str = match.group(1)
        table_name = match.group(2)
        
        columns = [col.strip() for col in columns_str.split(',') if col.strip()]
        
        return {
            "action": "select_columns",
            "table_name": table_name,
            "columns": columns
        }
    
    return {"error": "Invalid SELECT syntax. Use: SELECT * FROM table_name"}


def _parse_drop_table(sql):
    """
    Parse: DROP TABLE table_name
    """
    pattern = r"DROP TABLE (\w+)"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid DROP TABLE syntax. Use: DROP TABLE table_name"}
    
    return {
        "action": "drop_table",
        "table_name": match.group(1)
    }


def _parse_values(values_str):
    """
    Parse comma-separated values, handling quoted strings.
    """
    values = []
    current = ""
    in_quotes = False
    quote_char = None
    
    for char in values_str:
        if char in ("'", '"') and not in_quotes:
            in_quotes = True
            quote_char = char
            current += char
        elif char == quote_char and in_quotes:
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
        
        # Remove surrounding quotes if present
        if (val.startswith("'") and val.endswith("'")) or \
           (val.startswith('"') and val.endswith('"')):
            val = val[1:-1]
        
        # Try to convert to int or float
        try:
            if '.' in val:
                cleaned_values.append(float(val))
            else:
                cleaned_values.append(int(val))
        except ValueError:
            cleaned_values.append(val)
    
    return cleaned_values