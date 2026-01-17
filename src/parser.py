"""
SQL Parser for mini_rdbms
Supports: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE, JOIN
"""
import re

def parse_sql(sql_command):
    """
    Parse SQL commands.
    Supports: CREATE TABLE, INSERT, SELECT, UPDATE, DELETE, DROP TABLE, JOIN
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
    
    # UPDATE command
    elif sql_upper.startswith("UPDATE"):
        return _parse_update(sql)
    
    # DELETE command
    elif sql_upper.startswith("DELETE"):
        return _parse_delete(sql)
    
    # DROP TABLE command
    elif sql_upper.startswith("DROP TABLE"):
        return _parse_drop_table(sql)
    
    else:
        return {"error": f"Command not supported: {sql}"}


def _parse_create_table(sql):
    """
    Parse: CREATE TABLE table_name (col1, col2, ...)
    OR: CREATE TABLE table_name (col1 TYPE [CONSTRAINTS], col2 TYPE [CONSTRAINTS], ...)
    
    Supports both old style (no types) and new style (with types)
    """
    # Pattern: CREATE TABLE name (col1 TYPE, col2 TYPE CONSTRAINT, ...)
    pattern = r"CREATE TABLE (\w+)\s*\((.*)\)"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid CREATE TABLE syntax. Use: CREATE TABLE name (col1, col2, ...) or CREATE TABLE name (col1 TYPE, col2 TYPE, ...)"}
    
    table_name = match.group(1)
    columns_str = match.group(2)
    
    # Try to parse as new style (with data types) first
    result = _parse_columns_with_types(columns_str)
    if "columns" in result:
        # New style succeeded
        return {
            "action": "create_table",
            "table_name": table_name,
            "columns": result["columns"],
            "has_types": True
        }
    else:
        # If that fails, try old style (just column names)
        return _parse_columns_old_style(table_name, columns_str)


def _parse_columns_with_types(columns_str):
    """
    Parse columns with data types: id INT PRIMARY KEY, name VARCHAR(50), ...
    """
    columns = []
    column_defs = []
    
    # Split by commas, but handle nested parentheses (for DECIMAL(10,2))
    current = ""
    paren_depth = 0
    
    for char in columns_str:
        if char == '(':
            paren_depth += 1
            current += char
        elif char == ')':
            paren_depth -= 1
            current += char
        elif char == ',' and paren_depth == 0:
            column_defs.append(current.strip())
            current = ""
        else:
            current += char
    
    if current.strip():
        column_defs.append(current.strip())
    
    for col_def in column_defs:
        if not col_def:
            continue
            
        # Parse: "id INT PRIMARY KEY" or "name VARCHAR(50) NOT NULL"
        parts = col_def.strip().split()
        if len(parts) < 2:
            return {"error": f"Invalid column definition: {col_def}. Need: name TYPE [constraints]"}
        
        col_name = parts[0]
        col_type = parts[1].upper()
        
        # Check for VARCHAR - must have parentheses
        if col_type == "VARCHAR":
            return {"error": f"VARCHAR must specify length: VARCHAR(n)"}
        
        # Handle types with parameters: VARCHAR(50), DECIMAL(10,2)
        type_params = []
        if '(' in col_type:
            # Extract type name and parameters
            type_parts = col_type.split('(')
            base_type = type_parts[0]
            params_str = type_parts[1].rstrip(')')
            
            if base_type == "VARCHAR":
                try:
                    max_length = int(params_str)
                    col_type = f"VARCHAR({max_length})"
                    type_params = [max_length]
                except ValueError:
                    return {"error": f"Invalid VARCHAR length: {params_str}"}
            elif base_type == "DECIMAL":
                try:
                    if ',' in params_str:
                        precision, scale = map(int, params_str.split(','))
                    else:
                        precision = int(params_str)
                        scale = 0
                    col_type = f"DECIMAL({precision},{scale})"
                    type_params = [precision, scale]
                except:
                    return {"error": f"Invalid DECIMAL parameters: {params_str}"}
            else:
                return {"error": f"Unsupported parameterized type: {base_type}"}
        else:
            # Simple type without parameters
            base_type = col_type
        
        # Parse constraints (PRIMARY KEY, UNIQUE, NOT NULL)
        constraints = []
        i = 2  # Start after column name and type
        
        while i < len(parts):
            constraint = parts[i].upper()
            
            if constraint == "PRIMARY":
                if i + 1 < len(parts) and parts[i + 1].upper() == "KEY":
                    constraints.append("PRIMARY KEY")
                    i += 2
                else:
                    constraints.append("PRIMARY")
                    i += 1
            elif constraint == "NOT":
                if i + 1 < len(parts) and parts[i + 1].upper() == "NULL":
                    constraints.append("NOT NULL")
                    i += 2
                else:
                    constraints.append("NOT")
                    i += 1
            elif constraint in ["UNIQUE", "NULL"]:
                constraints.append(constraint)
                i += 1
            else:
                # Unknown word - might be part of type name (e.g., "DOUBLE PRECISION")
                # For now, skip it
                i += 1
        
        # Validate type
        valid_types = ["INT", "INTEGER", "VARCHAR", "TEXT", "STRING", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "BOOLEAN", "BOOL", "DATE", "DATETIME"]
        
        if base_type not in valid_types:
            return {"error": f"Unsupported data type: {col_type}. Supported: {', '.join(valid_types)}"}
        
        columns.append({
            "name": col_name,
            "type": col_type,
            "type_params": type_params,
            "constraints": constraints
        })
    
    if not columns:
        return {"error": "No columns specified"}
    
    return {"columns": columns}


def _parse_columns_old_style(table_name, columns_str):
    """
    Parse old style columns: just column names without types
    """
    columns = [col.strip() for col in columns_str.split(',') if col.strip()]
    
    if not columns:
        return {"error": "No columns specified"}
    
    # Convert old style to new style format (with generic "TEXT" type)
    typed_columns = []
    for col_name in columns:
        typed_columns.append({
            "name": col_name,
            "type": "TEXT",  # Default type for old syntax
            "type_params": [],
            "constraints": []
        })
    
    return {
        "action": "create_table",
        "table_name": table_name,
        "columns": typed_columns,
        "has_types": True  # Still mark as having types, just default ones
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
    Parse SELECT queries including:
    - SELECT * FROM table [WHERE condition]
    - SELECT columns FROM table [WHERE condition]  
    - SELECT * FROM table1 INNER JOIN table2 ON condition
    """
    # First check for JOIN pattern
    pattern_join = r"SELECT (.+)\s+FROM\s+(\w+)\s+INNER JOIN\s+(\w+)\s+ON\s+(.+?)(?:\s+WHERE\s+(.+))?$"
    match = re.match(pattern_join, sql, re.IGNORECASE)
    
    if match:
        columns_str = match.group(1).strip()
        table1 = match.group(2).strip()
        table2 = match.group(3).strip()
        join_condition = match.group(4).strip()
        where_clause = match.group(5).strip() if match.group(5) else None
        
        # Parse columns
        if columns_str == "*":
            columns = "*"
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        return {
            "action": "select_join",
            "tables": [table1, table2],
            "columns": columns,
            "join_condition": join_condition,
            "join_type": "INNER",
            "where": where_clause
        }
    
    # Pattern for SELECT * FROM table WHERE condition
    pattern_all = r"SELECT \*\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$"
    match = re.match(pattern_all, sql, re.IGNORECASE)
    
    if match:
        table_name = match.group(1)
        where_clause = match.group(2).strip() if match.group(2) else None
        return {
            "action": "select_all",
            "table_name": table_name,
            "columns": "*",
            "where": where_clause
        }
    
    # Pattern for SELECT columns FROM table WHERE condition
    pattern_cols = r"SELECT (.+)\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$"
    match = re.match(pattern_cols, sql, re.IGNORECASE)
    
    if match:
        columns_str = match.group(1)
        table_name = match.group(2)
        where_clause = match.group(3).strip() if match.group(3) else None
        
        columns = [col.strip() for col in columns_str.split(',') if col.strip()]
        
        return {
            "action": "select_columns",
            "table_name": table_name,
            "columns": columns,
            "where": where_clause
        }
    
    return {"error": "Invalid SELECT syntax. Use: SELECT * FROM table_name [WHERE condition] or SELECT * FROM table1 INNER JOIN table2 ON condition"}


def _parse_update(sql):
    """
    Parse: UPDATE table_name SET column=value WHERE condition
    Basic support: UPDATE users SET name='John' WHERE id=1
    """
    # Pattern: UPDATE table SET col=val WHERE condition
    pattern = r"UPDATE\s+(\w+)\s+SET\s+(.+?)(?:\s+WHERE\s+(.+))?$"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid UPDATE syntax. Use: UPDATE table SET column=value WHERE condition"}
    
    table_name = match.group(1)
    set_clause = match.group(2).strip()
    where_clause = match.group(3).strip() if match.group(3) else None
    
    # Parse SET clause (could be multiple: col1=val1, col2=val2)
    updates = {}
    set_parts = [part.strip() for part in set_clause.split(',')]
    
    for part in set_parts:
        if '=' not in part:
            return {"error": f"Invalid SET clause: {part}. Use: column=value"}
        
        col, val = part.split('=', 1)
        col = col.strip()
        val = val.strip()
        
        # Parse the value
        parsed_val = _parse_single_value(val)
        updates[col] = parsed_val
    
    if not updates:
        return {"error": "No SET values provided"}
    
    return {
        "action": "update",
        "table_name": table_name,
        "updates": updates,
        "where": where_clause
    }


def _parse_delete(sql):
    """
    Parse: DELETE FROM table_name WHERE condition
    Basic support: DELETE FROM users WHERE id=1
    """
    # Pattern: DELETE FROM table WHERE condition
    pattern = r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.+))?$"
    match = re.match(pattern, sql, re.IGNORECASE)
    
    if not match:
        return {"error": "Invalid DELETE syntax. Use: DELETE FROM table_name WHERE condition"}
    
    table_name = match.group(1)
    where_clause = match.group(2).strip() if match.group(2) else None
    
    return {
        "action": "delete",
        "table_name": table_name,
        "where": where_clause
    }


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
        
        # Parse single value
        parsed_val = _parse_single_value(val)
        cleaned_values.append(parsed_val)
    
    return cleaned_values


def _parse_single_value(val_str):
    """
    Parse a single value, converting to appropriate type.
    IMPORTANT: This does NOT evaluate expressions like "age+1"
    For UPDATE expressions, they will be handled in the database layer.
    """
    val = val_str.strip()
    
    # Remove surrounding quotes if present
    if (val.startswith("'") and val.endswith("'")) or \
       (val.startswith('"') and val.endswith('"')):
        return val[1:-1]
    
    # Try to convert to int or float
    try:
        if '.' in val:
            return float(val)
        else:
            return int(val)
    except ValueError:
        # Return as string (could be an expression like "age+1")
        return val