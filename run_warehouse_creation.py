import json
import subprocess
import sys
import os
from sqlalchemy import create_engine, text, exc

# --- Configuration Loading ---
def load_config(config_path='config.json'):
    """Loads configuration from a JSON file."""
    if not os.path.exists(config_path):
        print(f"Error: Configuration file '{config_path}' not found. Exiting.")
        sys.exit(1)
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        return config
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from '{config_path}'. Check its format. Exiting.")
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred while loading configuration: {e}. Exiting.")
        sys.exit(1)

# --- SQL Execution ---
def execute_sql_from_file(sql_file_path, db_config):
    """Executes SQL statements from a file."""
    if not db_config:
        print("Error: Database configuration is missing. Cannot execute SQL. Exiting.")
        sys.exit(1)
    
    db_host = db_config.get('host', 'mariadb')
    db_user = db_config.get('user', 'trainee')
    db_password = db_config.get('password', 'trainpass')
    db_name = db_config.get('database', 'warehouse')
    db_port = db_config.get('port', 3306)
    
    db_url = f"mysql+pymysql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
    
    if not os.path.exists(sql_file_path):
        print(f"Error: SQL file '{sql_file_path}' not found. Exiting.")
        sys.exit(1)

    try:
        engine = create_engine(db_url)
        with engine.connect() as connection:
            with open(sql_file_path, 'r') as file:
                sql_script = file.read()
            
            # Simple split by semicolon for basic SQL files like warehouse_schema.sql
            # More complex splitting might be needed for scripts with procedures/functions or comments within statements
            sql_statements = [stmt.strip() for stmt in sql_script.split(';') if stmt.strip()]
            
            print(f"\n--- Executing SQL from {sql_file_path} ---")
            for statement in sql_statements:
                if not statement:
                    continue
                try:
                    print(f"Executing: {statement[:100]}...") # Print start of statement
                    connection.execute(text(statement))
                    connection.commit() # Commit after each statement or group of related statements
                    print("Success.")
                except exc.SQLAlchemyError as e:
                    print(f"Error executing statement: {statement[:100]}...\n{e}")
                    # Decide if you want to stop on error or continue
                    # For schema creation, it's often best to stop.
                    print("Aborting SQL execution due to error.")
                    return False # Indicate failure
            print(f"--- Successfully executed SQL from {sql_file_path} ---")
            return True # Indicate success
    except exc.SQLAlchemyError as e:
        print(f"Database connection error: {e}")
        return False # Indicate failure
    except Exception as e:
        print(f"An unexpected error occurred during SQL execution: {e}")
        return False # Indicate failure

# --- Python Script Execution ---
def run_python_script(script_path, base_dir="."):
    """Runs a Python script using subprocess."""
    full_script_path = os.path.join(base_dir, script_path)
    if not os.path.exists(full_script_path):
        print(f"Error: Python script '{full_script_path}' not found. Skipping.")
        return False

    print(f"\n--- Running Python script: {script_path} ---")
    try:
        # Ensure that the script is found relative to the current working directory if needed
        # or adjust `cwd` parameter in subprocess.run
        process = subprocess.run(
            [sys.executable, full_script_path], 
            check=True, 
            capture_output=True, 
            text=True,
            cwd=base_dir # Run script from the project's root directory
        )
        print(f"Output from {script_path}:")
        if process.stdout:
            print("STDOUT:\n", process.stdout)
        if process.stderr: # Should be empty if check=True and no errors, but good to check
            print("STDERR:\n", process.stderr)
        print(f"--- Successfully ran {script_path} ---")
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running script {script_path}:")
        print(f"Return code: {e.returncode}")
        if e.stdout:
            print("STDOUT:\n", e.stdout)
        if e.stderr:
            print("STDERR:\n", e.stderr)
        print(f"--- Failed to run {script_path} ---")
        return False
    except Exception as e:
        print(f"An unexpected error occurred while trying to run {script_path}: {e}")
        return False

# --- Main Orchestration ---
if __name__ == '__main__':
    print("--- Starting Warehouse Creation Process ---")
    
    config_data = load_config()
    db_config = config_data.get('database_config')
    
    if not db_config:
        print("Error: 'database_config' not found in config.json. Exiting.")
        sys.exit(1)

    warehouse_schema_sql_path = "warehouse_schema.sql"
    
    # Step 1: Execute SQL Schema
    if not execute_sql_from_file(warehouse_schema_sql_path, db_config):
        print("Failed to execute warehouse schema. Aborting further steps.")
        sys.exit(1)
        
    # Step 2: Run Python Data Generation Scripts
    # Ensure these scripts are in the root directory or adjust paths accordingly.
    scripts_to_run = [
        "load_dim_dates.py",
        "create_dim_departments.py",
        "create_dim_report_grade.py",
        "CreatePeople.py",       # As per previous file listing
        "create students.py",
        "create staff.py",
        "create attendance.py",
        "create report data.py",
        "Create pivoted report card" # As per previous file listing
    ]
    
    all_scripts_succeeded = True
    for script_name in scripts_to_run:
        if not run_python_script(script_name):
            all_scripts_succeeded = False
            print(f"Stopping execution due to failure in {script_name}.")
            break # Stop on first script failure
            
    if all_scripts_succeeded:
        print("\n--- Warehouse Creation Process Completed Successfully ---")
    else:
        print("\n--- Warehouse Creation Process Encountered Errors ---")
