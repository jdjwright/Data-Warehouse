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
            print("Creating default report grades from config...")
            school_params = config.get('school_parameters', {})
            default_grades_list = school_params.get('default_report_grades_list')

            if not default_grades_list:
                print("Error: 'default_report_grades_list' not found or empty in config['school_parameters']. Cannot create default report grades.")
                return # Or raise an error
            
            df_default_grades = pd.DataFrame(default_grades_list)

            # Ensure all required columns from the schema are present, adding them with defaults if missing
            # Schema columns: id (auto), result, numerical_result, ucas_points, gis_points, data_type, printable_result, hex_colour, hex_colour_text, description, total_grades, igcse_pass, category
            
            # Core columns from config: result, numerical_result, data_type, category, ucas_points, gis_points, printable_result, hex_colour, hex_colour_text, description, total_grades, igcse_pass
            # The config now provides values (even if null) for most of these.
            # This logic will ensure they exist if the config list was missing some, or set defaults for any totally new schema columns.
            
            schema_columns_with_defaults = {
                'ucas_points': None,
                'gis_points': None,
                'printable_result': df_default_grades['result'] if 'result' in df_default_grades.columns else None, # Default to 'result'
                'hex_colour': None,
                'hex_colour_text': None,
                'description': (df_default_grades['result'] + " - " + df_default_grades['category']) if ('result' in df_default_grades.columns and 'category' in df_default_grades.columns) else 'Default Description',
                'total_grades': 1,
                'igcse_pass': None # boolean/int
            }

            for col, default_value in schema_columns_with_defaults.items():
                if col not in df_default_grades.columns:
                    df_default_grades[col] = default_value
            
            # Ensure base columns are present if by some chance the config list was malformed for a specific record
            base_required_cols = ['result', 'numerical_result', 'data_type', 'category']
            for col in base_required_cols:
                 if col not in df_default_grades.columns:
                     df_default_grades[col] = None # Or a more specific default if appropriate
                     print(f"Warning: Column '{col}' was missing from default_report_grades_list items. Added with None.")


            df_default_grades.to_sql(table_name, con=engine, if_exists='replace', index=False)
            rows_loaded = len(df_default_grades)
            print(f"Successfully created and loaded {rows_loaded} default rows into '{table_name}' table from config.")

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
    except KeyError as e:
        print(f"Error: Missing key {e} in configuration. Cannot create default report grades.")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}")
    except exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    create_report_grades()
