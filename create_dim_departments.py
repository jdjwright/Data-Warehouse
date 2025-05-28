import json
import pandas as pd
from sqlalchemy import create_engine, exc
import os

def create_departments(config_path='config.json'):
    """
    Loads department data from a CSV file or creates default departments
    and loads them into the dim_Departments table in a MySQL database.

    Args:
        config_path (str): Path to the configuration file.
    """
    try:
        # Read configuration
        with open(config_path, 'r') as f:
            config = json.load(f)

        db_config = config.get('database_config', {})
        host = db_config.get('host')
        user = db_config.get('user')
        password = db_config.get('password')
        database = db_config.get('database')
        port = db_config.get('port')

        data_files = config.get('data_file_paths', {})
        csv_path = data_files.get('dim_departments_csv_path') # Can be None

        if not all([host, user, password, database, port]):
            print("Error: Database configuration is incomplete.")
            return

        # Construct database connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        table_name = 'dim_departments' # Changed to lowercase
        df_to_load = None
        rows_loaded = 0

        # Conditional Loading/Creation
        load_from_csv = False
        if csv_path:
            if os.path.exists(csv_path):
                try:
                    df_to_load = pd.read_csv(csv_path)
                    if not df_to_load.empty:
                        load_from_csv = True
                        print(f"Successfully read {len(df_to_load)} rows from CSV file: {csv_path}")
                    else:
                        print(f"Warning: CSV file {csv_path} is empty. Will attempt to create default departments.")
                except pd.errors.EmptyDataError:
                    print(f"Warning: CSV file {csv_path} is empty. Will attempt to create default departments.")
                except pd.errors.ParserError:
                    print(f"Warning: Could not parse CSV file {csv_path}. Check its format. Will attempt to create default departments.")
                except Exception as e:
                    print(f"Warning: An error occurred reading {csv_path}: {e}. Will attempt to create default departments.")
            else:
                print(f"Warning: CSV file specified but not found at {csv_path}. Will create default departments.")
        else:
            print("No CSV path provided for departments. Will create default departments.")

        if load_from_csv and df_to_load is not None:
            df_to_load.to_sql(table_name, con=engine, if_exists='replace', index=False)
            rows_loaded = len(df_to_load)
            print(f"Successfully loaded {rows_loaded} rows into '{table_name}' table from {csv_path}.")
        else:
            print("Creating default departments from config...")
            school_params = config.get('school_parameters', {})
            default_depts_list = school_params.get('default_department_list')

            if not default_depts_list:
                print("Error: 'default_department_list' not found or empty in config['school_parameters']. Cannot create default departments.")
                return # Or raise an error

            df_default_depts = pd.DataFrame(default_depts_list)
            # Ensure required columns are present, if not, an error will be raised by to_sql or DataFrame creation
            # For this table, isams_subject_code, subject_name, report_name are expected.
            
            df_default_depts.to_sql(table_name, con=engine, if_exists='replace', index=False)
            rows_loaded = len(df_default_depts)
            print(f"Successfully created and loaded {rows_loaded} default rows into '{table_name}' table from config.")

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
    except KeyError as e:
        print(f"Error: Missing key {e} in configuration. Cannot create default departments.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}")
    except exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    # Placeholder for dependency checks if needed in a more complex setup
    # try:
    #     import pandas
    #     import sqlalchemy
    #     import pymysql
    # except ImportError:
    #     print("Attempting to install missing libraries: pandas, sqlalchemy, pymysql")
    #     import subprocess
    #     import sys
    #     try:
    #         subprocess.check_call([sys.executable, "-m", "pip", "install", "pandas", "sqlalchemy", "pymysql"])
    #     except subprocess.CalledProcessError as e:
    #         print(f"Failed to install libraries: {e}")
    #         sys.exit(1)
    create_departments()
