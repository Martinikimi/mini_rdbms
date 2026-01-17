"""
Todo List Web Application
Demonstrates mini_rdbms in action with a simple CRUD app
"""
import os
import sys
from pathlib import Path

# Add parent directory to path to import mini_rdbms
sys.path.insert(0, str(Path(__file__).parent.parent))

from flask import Flask, request, jsonify, render_template
from src.executor import SQLExecutor

app = Flask(__name__)
executor = SQLExecutor()

def init_database():
    """Initialize database with todos table if it doesn't exist."""
    try:
        # Check if todos table exists
        tables = executor.db.list_tables()
        
        if 'todos' not in tables:
            print("Creating todos table...")
            
            # Use simple CREATE TABLE syntax that works with your parser
            create_query = "CREATE TABLE todos (id INT, task TEXT, completed BOOLEAN)"
            result = executor.execute(create_query)
            print(f"Table creation: {result}")
            
            if isinstance(result, str) and "Error:" in result:
                print("Failed to create table with first syntax, trying alternative...")
                # Try even simpler syntax
                create_query = "CREATE TABLE todos (id, task, completed)"
                result = executor.execute(create_query)
                print(f"Alternative creation: {result}")
            
            # Add sample todos
            sample_todos = [
                "Learn about databases",
                "Build a mini RDBMS", 
                "Create a web app demo",
                "Submit the challenge"
            ]
            
            for i, task in enumerate(sample_todos, 1):
                # Simple INSERT without types
                insert_query = f"INSERT INTO todos VALUES ({i}, '{task}', false)"
                insert_result = executor.execute(insert_query)
                if isinstance(insert_result, str) and "Inserted into" in insert_result:
                    print(f"  Added: {task}")
                else:
                    print(f"  Failed to add: {task} - {insert_result}")
            
            print("✅ Database initialized with sample data")
        else:
            print("✅ Todos table already exists")
            
    except Exception as e:
        print(f"Error initializing database: {e}")
        import traceback
        traceback.print_exc()

# Initialize database on startup
init_database()

@app.route('/')
def index():
    """Render the main todo app page."""
    return render_template('index.html')

@app.route('/api/todos', methods=['GET'])
def get_todos():
    """Get all todos from the database."""
    try:
        result = executor.execute("SELECT * FROM todos")
        
        if isinstance(result, str):
            # Check if it's an error message
            if "Error:" in result:
                return jsonify({'success': False, 'error': result}), 500
            # Might be a success message string
            return jsonify({'success': True, 'todos': []})
        
        # Format todos as list of dictionaries
        todos = []
        if isinstance(result, list):
            for row in result:
                if len(row) >= 3:  # Should have id, task, completed
                    # Handle different boolean representations
                    completed = False
                    if row[2] is True:
                        completed = True
                    elif isinstance(row[2], str) and row[2].lower() in ['true', '1', 'yes']:
                        completed = True
                    elif isinstance(row[2], (int, float)) and row[2] != 0:
                        completed = True
                    
                    todos.append({
                        'id': row[0],
                        'task': row[1],
                        'completed': completed
                    })
                elif len(row) == 2:  # If no completed column
                    todos.append({
                        'id': row[0],
                        'task': row[1],
                        'completed': False
                    })
        
        return jsonify({'success': True, 'todos': todos})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/todos', methods=['POST'])
def add_todo():
    """Add a new todo."""
    try:
        data = request.json
        task = data.get('task', '').strip()
        
        if not task:
            return jsonify({'success': False, 'error': 'Task cannot be empty'}), 400
        
        # Escape single quotes in task
        task = task.replace("'", "''")
        
        # Get the next ID
        result = executor.execute("SELECT id FROM todos")
        next_id = 1
        
        if isinstance(result, list) and result:
            # Find max ID
            ids = []
            for row in result:
                if row and len(row) > 0 and row[0] is not None:
                    try:
                        ids.append(int(row[0]))
                    except:
                        pass
            
            if ids:
                next_id = max(ids) + 1
        
        # Insert the new todo
        sql = f"INSERT INTO todos VALUES ({next_id}, '{task}', false)"
        result = executor.execute(sql)
        
        if isinstance(result, str) and ("Inserted into" in result or "Inserted" in result):
            return jsonify({
                'success': True, 
                'todo': {'id': next_id, 'task': task.replace("''", "'"), 'completed': False}
            })
        else:
            # Try without the false value (in case completed column doesn't exist)
            sql = f"INSERT INTO todos VALUES ({next_id}, '{task}')"
            result = executor.execute(sql)
            
            if isinstance(result, str) and ("Inserted into" in result or "Inserted" in result):
                return jsonify({
                    'success': True, 
                    'todo': {'id': next_id, 'task': task.replace("''", "'"), 'completed': False}
                })
            else:
                return jsonify({'success': False, 'error': str(result)}), 500
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/todos/<int:todo_id>', methods=['PUT'])
def update_todo(todo_id):
    """Update a todo (toggle completion)."""
    try:
        data = request.json
        completed = data.get('completed', False)
        
        # Convert boolean to string representation your database understands
        completed_str = 'true' if completed else 'false'
        
        sql = f"UPDATE todos SET completed = {completed_str} WHERE id = {todo_id}"
        result = executor.execute(sql)
        
        if isinstance(result, str) and ("Updated" in result or "Update" in result):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': str(result)}), 500
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/todos/<int:todo_id>', methods=['DELETE'])
def delete_todo(todo_id):
    """Delete a todo."""
    try:
        sql = f"DELETE FROM todos WHERE id = {todo_id}"
        result = executor.execute(sql)
        
        if isinstance(result, str) and ("Deleted" in result or "Delete" in result):
            return jsonify({'success': True})
        else:
            return jsonify({'success': False, 'error': str(result)}), 500
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get todo statistics."""
    try:
        # Get total todos
        result = executor.execute("SELECT COUNT(*) FROM todos")
        total = 0
        if isinstance(result, list) and result and result[0]:
            total = result[0][0] if isinstance(result[0][0], (int, float)) else 0
        
        # Get completed todos
        result = executor.execute("SELECT COUNT(*) FROM todos WHERE completed = true")
        completed = 0
        if isinstance(result, list) and result and result[0]:
            completed = result[0][0] if isinstance(result[0][0], (int, float)) else 0
        
        return jsonify({
            'success': True,
            'stats': {
                'total': int(total),
                'completed': int(completed),
                'pending': int(total - completed)
            }
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Check if the database is working."""
    try:
        tables = executor.db.list_tables()
        return jsonify({
            'success': True,
            'database': 'mini_rdbms',
            'tables': tables,
            'status': 'operational' if tables else 'no tables'
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

@app.route('/api/debug', methods=['GET'])
def debug_info():
    """Debug endpoint to see raw database state."""
    try:
        # Get raw todos data
        result = executor.execute("SELECT * FROM todos")
        
        # Get table info
        table_info = None
        if executor.db.table_exists('todos'):
            table_info = executor.db.get_table_info('todos')
        
        return jsonify({
            'success': True,
            'raw_todos': result if isinstance(result, list) else str(result),
            'table_info': table_info,
            'table_exists': executor.db.table_exists('todos')
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'success': False, 'error': str(e)}), 500

if __name__ == '__main__':
    print("=" * 50)
    print("Starting Todo App - Powered by mini_rdbms")
    print("=" * 50)
    print(f"Database files: {os.path.abspath('../data')}")
    print("Access the app at: http://localhost:5000")
    print("Debug info at: http://localhost:5000/api/debug")
    print("=" * 50)
    app.run(debug=True, port=5000)