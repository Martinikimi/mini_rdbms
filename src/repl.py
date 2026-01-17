"""
Interactive REPL for mini_rdbms - Windows compatible version
Command-line interface similar to mysql or psql
"""
import sys
import os
from src.executor import SQLExecutor

class MiniRDBMS_REPL:
    def __init__(self, data_dir="data"):
        self.executor = SQLExecutor(data_dir)
        self.data_dir = data_dir
        self.running = True
        self.command_history = []
        self.history_index = -1
        
        # Create data directory if it doesn't exist
        if not os.path.exists(data_dir):
            os.makedirs(data_dir)
        
        # Try to load command history from file
        self.history_file = os.path.join(data_dir, ".repl_history")
        self._load_history()
    
    def _load_history(self):
        """Load command history from file."""
        try:
            if os.path.exists(self.history_file):
                with open(self.history_file, 'r', encoding='utf-8') as f:
                    self.command_history = [line.strip() for line in f if line.strip()]
        except Exception:
            self.command_history = []  # Start with empty history
    
    def _save_history(self):
        """Save command history to file."""
        try:
            with open(self.history_file, 'w', encoding='utf-8') as f:
                for cmd in self.command_history[-100:]:  # Keep last 100 commands
                    f.write(cmd + "\n")
        except Exception:
            pass  # Can't save history, but that's OK
    
    def _get_input(self, prompt):
        """Get input from user with basic up/down arrow support."""
        try:
            # Simple input for now - could enhance with pyreadline3 on Windows
            return input(prompt)
        except KeyboardInterrupt:
            raise
        except EOFError:
            raise
    
    def print_banner(self):
        """Print welcome banner."""
        print("\n" + "=" * 60)
        print("  mini_rdbms v1.0 - Interactive SQL Shell")
        print("=" * 60)
        print("Type SQL commands ending with ';'")
        print("Type 'help;' for help, 'exit;' or 'quit;' to exit")
        print("=" * 60 + "\n")
    
    def print_help(self):
        """Print help information."""
        help_text = """
SQL COMMANDS:
  CREATE TABLE table_name (col1 TYPE, col2 TYPE, ...)
  DROP TABLE table_name
  INSERT INTO table_name VALUES (val1, val2, ...)
  SELECT * FROM table_name
  SELECT col1, col2 FROM table_name
  UPDATE table_name SET col=val WHERE condition
  DELETE FROM table_name WHERE condition

DATA TYPES:
  INT, INTEGER          - Whole numbers
  VARCHAR(n)            - Strings up to n characters
  DECIMAL(p,s)          - Decimal numbers with precision
  BOOLEAN, BOOL         - True/False values
  TEXT, STRING          - Text strings
  FLOAT, REAL           - Floating point numbers
  DATE, DATETIME        - Date and time values

CONSTRAINTS:
  PRIMARY KEY           - Unique identifier for each row
  UNIQUE                - No duplicate values allowed
  NOT NULL              - Column cannot be NULL
  NULL                  - Column can be NULL (default)

REPL COMMANDS:
  help;                 - Show this help message
  show tables;          - List all tables in database
  describe table_name;  - Show table structure
  exit; or quit;        - Exit the REPL
  clear;                - Clear screen
  history;              - Show command history

EXAMPLES:
  CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50), age INT);
  INSERT INTO users VALUES (1, 'Alice', 30);
  SELECT * FROM users;
  UPDATE users SET age=31 WHERE name='Alice';
  DELETE FROM users WHERE id=1;
"""
        print(help_text)
    
    def execute_special_command(self, command):
        """Handle special REPL commands (not SQL)."""
        cmd_lower = command.strip().lower()
        
        if cmd_lower == "help":
            self.print_help()
            return True
        elif cmd_lower == "show tables":
            tables = self.executor.db.list_tables()
            if not tables:
                print("No tables in database.")
            else:
                print("\nTables in database:")
                print("-" * 40)
                for table in tables:
                    count = self.executor.db.count_rows(table)
                    print(f"{table:20} {count:4} rows")
            return True
        elif cmd_lower.startswith("describe "):
            parts = command.split()
            if len(parts) == 2:
                table_name = parts[1]
                self._describe_table(table_name)
            else:
                print("Usage: describe table_name;")
            return True
        elif cmd_lower in ["exit", "quit"]:
            print("Goodbye!")
            self.running = False
            return True
        elif cmd_lower == "clear":
            os.system('cls')
            return True
        elif cmd_lower == "history":
            print("\nCommand History:")
            print("-" * 40)
            for i, cmd in enumerate(self.command_history[-20:], 1):  # Last 20 commands
                print(f"{i:3}: {cmd}")
            return True
        
        return False
    
    def _describe_table(self, table_name):
        """Show table structure."""
        table_info = self.executor.db.get_table_info(table_name)
        if not table_info:
            print(f"Table '{table_name}' does not exist.")
            return
        
        print(f"\nTable: {table_name}")
        print("-" * 60)
        
        # Get column definitions
        column_defs = self.executor.db.get_column_definitions(table_name)
        if not column_defs:
            print("Could not retrieve column information.")
            return
        
        print(f"{'Column':20} {'Type':20} {'Constraints'}")
        print("-" * 60)
        
        for col in column_defs:
            col_name = col["name"]
            col_type = col["type"]
            
            # Add type parameters if present
            if col["type_params"]:
                if col_type.startswith("VARCHAR"):
                    col_type = f"VARCHAR({col['type_params'][0]})"
                elif col_type.startswith("DECIMAL"):
                    col_type = f"DECIMAL({col['type_params'][0]},{col['type_params'][1]})"
            
            constraints = " ".join(col["constraints"]) if col["constraints"] else ""
            print(f"{col_name:20} {col_type:20} {constraints}")
        
        row_count = len(table_info["data"])
        print(f"\nTotal rows: {row_count}")
    
    def format_result(self, result):
        """Format execution result for display."""
        if isinstance(result, list):
            if not result:
                return "No rows found."
            
            # Try to format as table if possible
            if result and isinstance(result[0], list):
                return self._format_table(result)
            
            # Simple list format
            output = []
            for i, row in enumerate(result, 1):
                output.append(f"{i}. {row}")
            return "\n".join(output)
        
        return str(result)
    
    def _format_table(self, rows):
        """Format query results as a table."""
        if not rows:
            return "Empty result set."
        
        # Find max width for each column
        col_widths = []
        for i in range(len(rows[0])):
            max_width = max(len(str(row[i])) for row in rows)
            col_widths.append(min(max_width, 50))  # Limit to 50 chars
        
        # Create header separator
        total_width = sum(col_widths) + (3 * len(col_widths)) - 1
        separator = "-" * min(total_width, 100)  # Limit width
        
        # Build table output
        output = []
        output.append(separator)
        
        # Add rows
        for i, row in enumerate(rows, 1):
            row_str = f"{i:3} | "
            for j, cell in enumerate(row):
                cell_str = str(cell)
                if len(cell_str) > 50:
                    cell_str = cell_str[:47] + "..."
                row_str += f"{cell_str:{col_widths[j]}} | "
            output.append(row_str.rstrip())
            if i == 1:  # Add separator after first row
                output.append(separator)
        
        output.append(separator)
        output.append(f"Total: {len(rows)} rows")
        
        return "\n".join(output)
    
    def run(self):
        """Run the REPL."""
        self.print_banner()
        
        current_command = ""
        
        while self.running:
            try:
                # Get input with prompt
                if not current_command:
                    prompt = "mini_rdbms> "
                else:
                    prompt = "      ...> "
                
                line = self._get_input(prompt).strip()
                
                # Skip empty lines
                if not line:
                    continue
                
                # Add to current command
                current_command += line + " "
                
                # Check if command ends with semicolon
                if current_command.strip().endswith(";"):
                    # Remove trailing semicolon and whitespace
                    full_command = current_command.strip()[:-1].strip()
                    current_command = ""  # Reset for next command
                    
                    # Skip empty commands
                    if not full_command:
                        continue
                    
                    # Add to history (if not already there)
                    if not self.command_history or self.command_history[-1] != full_command:
                        self.command_history.append(full_command)
                    
                    # Check for special commands first
                    if self.execute_special_command(full_command):
                        continue
                    
                    # Execute SQL command
                    print(f"Executing: {full_command}")
                    result = self.executor.execute(full_command)
                    
                    # Format and display result
                    formatted_result = self.format_result(result)
                    print(formatted_result)
                    
            except KeyboardInterrupt:
                print("\nInterrupted. Type 'exit;' to quit or continue with SQL.")
                current_command = ""  # Reset current command
                continue
            except EOFError:
                print("\nGoodbye!")
                break
            except Exception as e:
                print(f"Error: {e}")
                current_command = ""  # Reset on error
        
        # Save history on exit
        self._save_history()

def main():
    """Main entry point for the REPL."""
    import sys
    
    # Get data directory from command line or use default
    data_dir = "data"
    if len(sys.argv) > 1:
        data_dir = sys.argv[1]
    
    # Create and run REPL
    repl = MiniRDBMS_REPL(data_dir)
    repl.run()

if __name__ == "__main__":
    main()