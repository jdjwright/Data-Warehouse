import json
import pandas as pd
from sqlalchemy import create_engine, exc
import os

def create_report_grades(config_path='config.json'):
    """
    Loads report grade data from a CSV file or creates default report grades
    and loads them into the dim_report_grade table in a MySQL database.

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
        csv_path = data_files.get('dim_report_grade_csv_path') # Can be None

        if not all([host, user, password, database, port]):
            print("Error: Database configuration is incomplete.")
            return

        # Construct database connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(connection_string)
        table_name = 'dim_report_grade'
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
                        print(f"Warning: CSV file {csv_path} is empty. Will attempt to create default report grades.")
                except pd.errors.EmptyDataError:
                    print(f"Warning: CSV file {csv_path} is empty. Will attempt to create default report grades.")
                except pd.errors.ParserError:
                    print(f"Warning: Could not parse CSV file {csv_path}. Check its format. Will attempt to create default report grades.")
                except Exception as e:
                    print(f"Warning: An error occurred reading {csv_path}: {e}. Will attempt to create default report grades.")
            else:
                print(f"Warning: CSV file specified but not found at {csv_path}. Will create default report grades.")
        else:
            print("No CSV path provided for report grades. Will create default report grades.")

        if load_from_csv and df_to_load is not None:
            # Ensure all required columns are present when loading from CSV, adding them with None if missing
            # This is a basic check; a more robust solution might involve schema validation
            required_cols = ['result', 'numerical_result', 'ucas_points', 'gis_points', 'data_type', 'printable_result', 'hex_colour', 'hex_colour_text', 'description', 'total_grades', 'igcse_pass', 'category']
            for col in required_cols:
                if col not in df_to_load.columns:
                    df_to_load[col] = None # Or an appropriate default based on schema
            
            df_to_load.to_sql(table_name, con=engine, if_exists='replace', index=False)
            rows_loaded = len(df_to_load)
            print(f"Successfully loaded {rows_loaded} rows into '{table_name}' table from {csv_path}.")
        else:
            print("Creating default report grades...")
            default_grades_data = {
                'result': ['A*', 'A', 'B', 'C', 'D', 'E', 'U', '9', '8', '7', '6', '5', '4', '3', '2', '1', 'Pass', 'Fail', 'Merit', 'Distinction'],
                'numerical_result': [9.5, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 9.0, 8.0, 7.0, 6.0, 5.0, 4.0, 3.0, 2.0, 1.0, 1.0, 0.0, 2.0, 3.0],
                'data_type': ['Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Attainment', 'Generic', 'Generic', 'Generic', 'Generic'],
                'category': ['Single grades', 'Single grades', 'Single grades', 'Single grades', 'Single grades', 'Single grades', 'Single grades', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'GCSE 9-1', 'Pass/Fail', 'Pass/Fail', 'BTEC', 'BTEC']
            }
            df_default_grades = pd.DataFrame(default_grades_data)

            # Add any missing columns with default values as per schema
            # Schema columns: id (auto), result, numerical_result, ucas_points, gis_points, data_type, printable_result, hex_colour, hex_colour_text, description, total_grades, igcse_pass, category
            df_default_grades['ucas_points'] = None
            df_default_grades['gis_points'] = None
            df_default_grades['printable_result'] = df_default_grades['result'] # Default printable_result to be same as result
            df_default_grades['hex_colour'] = None
            df_default_grades['hex_colour_text'] = None
            df_default_grades['description'] = df_default_grades['result'] + " - " + df_default_grades['category'] # Example description
            df_default_grades['total_grades'] = 1 # Default to 1
            df_default_grades['igcse_pass'] = None # Could be boolean based on result/category logic if needed

            df_default_grades.to_sql(table_name, con=engine, if_exists='replace', index=False)
            rows_loaded = len(df_default_grades)
            print(f"Successfully created and loaded {rows_loaded} default rows into '{table_name}' table.")

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}")
    except exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    create_report_grades()
