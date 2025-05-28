import json
import pandas as pd
from sqlalchemy import create_engine, exc
import os

def load_dates_from_csv(config_path='config.json'):
    """
    Loads data from a CSV file into the dim_Dates table in a MySQL database.

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
        csv_path = data_files.get('dim_dates_csv_path')

        if not all([host, user, password, database, port]):
            print("Error: Database configuration is incomplete.")
            return

        if not csv_path:
            print(f"Error: 'dim_dates_csv_path' not found in {config_path} under 'data_file_paths'.")
            return

        # Construct database connection string
        connection_string = f"mysql+pymysql://{user}:{password}@{host}:{port}/{database}"

        # Create SQLAlchemy engine
        engine = create_engine(connection_string)

        # Check if the CSV file exists
        if not os.path.exists(csv_path):
            print(f"Error: CSV file not found at {csv_path}")
            return

        # Read CSV file into a pandas DataFrame
        df = pd.read_csv(csv_path)

        # Write DataFrame to dim_dates table
        table_name = 'dim_dates' # Changed to lowercase
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)

        print(f"Successfully loaded {len(df)} rows into '{table_name}' table.")

    except FileNotFoundError:
        print(f"Error: Configuration file not found at {config_path}")
    except json.JSONDecodeError:
        print(f"Error: Could not decode JSON from {config_path}")
    except exc.SQLAlchemyError as e:
        print(f"Database error: {e}")
    except pd.errors.EmptyDataError:
        print(f"Error: The CSV file at {csv_path} is empty.")
    except pd.errors.ParserError:
        print(f"Error: Could not parse the CSV file at {csv_path}. Check its format.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

if __name__ == '__main__':
    # Install necessary libraries if they are not present.
    # This is a placeholder for potential future dependency management.
    # For now, we assume pandas, sqlalchemy, and pymysql are installed.
    # Example:
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

    load_dates_from_csv()
