import pandas as pd
from sqlalchemy import create_engine

# Define your PostgreSQL connection parameters
username = 'postgres'
password = 'Jagadish%4025'  # URL encoded password for Jagadish@25
database = 'My_DB'
host = 'localhost'  # or '127.0.0.1'
port = '5432'  # default PostgreSQL port

# Path to your CSV file
csv_file_path = r'C:\Users\91932\OneDrive\Documents\WORK\DATA_ENGINEER_ASGN_1\final_dataframe_csv.csv'  # Replace with your actual CSV file path

try:
    # Create a database connection
    engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{database}')

    # Read the CSV file into a DataFrame
    df = pd.read_csv(csv_file_path)

    # Write the DataFrame to PostgreSQL
    df.to_sql('comprehensive', engine, if_exists='replace', index=False)

    print("Data has been written to the comprehensive table.")

except Exception as e:
    print(f"An error occurred: {e}")
