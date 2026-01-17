#!/usr/bin/env python3
"""
mini_rdbms - Interactive SQL Shell Launcher
"""
import sys
import os

# Add the src directory to Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, current_dir)  # Add current directory first

try:
    from src.repl import main
    main()
except ImportError as e:
    print(f"Error: Could not import REPL module: {e}")
    print("Current directory:", os.getcwd())
    print("Python path:", sys.path)
    sys.exit(1)