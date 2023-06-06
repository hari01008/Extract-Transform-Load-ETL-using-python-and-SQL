import tkinter as tk
from tkinter import filedialog
import subprocess
import pandas as pd
from sqlalchemy import create_engine
import json
import mysql.connector
import os


# Extraction Function
def extract_data():
    # Mysql Connection
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="hari2"
    )

    # Define query to retrieve all table names in the database
    table_query = "SHOW TABLES"

    # Execute the query and fetch the data
    cursor = db_connection.cursor()
    cursor.execute(table_query)
    tables = cursor.fetchall()
        
    # Create a folder named "extracted_data" if it doesn't exist
    if not os.path.exists("extracted_data"):
        os.mkdir("extracted_data")

    # Loop through each table and retrieve its data
    for table in tables:
        table_name = table[0]

        # Define query to retrieve data from table
        data_query = f"SELECT * FROM {table_name}"

        # Execute the query and fetch the data
        cursor.execute(data_query)
        data = cursor.fetchall()

        # Define column names for the CSV file
        column_names_query = f"SHOW COLUMNS FROM {table_name}"
        cursor.execute(column_names_query)
        columns = [column[0] for column in cursor.fetchall()]

        # Create a DataFrame using the fetched data and column names
        df = pd.DataFrame(data, columns=columns)

        # Export the DataFrame to a CSV file inside the "extracted_data" folder with table name as file name
        file_name = f"{table_name}.csv"
        file_path = os.path.join("extracted_data", file_name)
        df.to_csv(file_path, index=False)
        print(f"The Extraction Process for table {table_name} was done successfully and the output stored as {file_path}")

    # Close the database connection
    cursor.close()
    db_connection.close()
    print("Data Extraction Completed.")


# Transformation Function
def transform_data():
    # Load the JSON config file
    config_file = "config.json"  # Update the config file name or path if necessary
    with open(config_file) as f:
        config = json.load(f)

    # Connect to the MySQL database using SQLAlchemy
    db_connection = create_engine(
        f"mysql+mysqlconnector://{config['database']['username']}:{config['database']['password']}@{config['database']['host']}:{config['database']['port']}/{config['database']['database']}")

    # Load the data from the tables specified in the config
    df_customers = pd.read_sql(f"SELECT * FROM {config['tables']['transformation'][0]['source_table']}",
                               db_connection)
    df_orders = pd.read_sql(f"SELECT * FROM {config['tables']['transformation'][1]['source_table']}",
                            db_connection)

    # Perform transformations specified in the config
    # 1. Change date format
    df_orders[config['tables']['transformation'][1]['target_column'][0]] = pd.to_datetime(
        df_orders[config['tables']['transformation'][1]['source_column']],
        format=config['tables']['transformation'][1]['date_format']).dt.year
    df_orders[config['tables']['transformation'][1]['target_column'][1]] = pd.to_datetime(
        df_orders[config['tables']['transformation'][1]['source_column']],
        format=config['tables']['transformation'][1]['date_format']).dt.month
    df_orders[config['tables']['transformation'][1]['target_column'][2]] = pd.to_datetime(
        df_orders[config['tables']['transformation'][1]['source_column']],
        format=config['tables']['transformation'][1]['date_format']).dt.day

    # 2. Concatenate columns
    df_customers[config['tables']['transformation'][0]['target_column']] = df_customers[
                                                                               config['tables']['transformation'][0][
                                                                                   'source_column'][0]] + ' ' + \
                                                                           df_customers[
                                                                               config['tables']['transformation'][0][
                                                                                   'source_column'][1]]

    # 3. Drop columns
    df_customers.drop(columns=config['tables']['transformation'][2]['drop_columns'], inplace=True)

    # 4. Join tables
    df_denormalized = pd.merge(df_customers, df_orders,
                               how=config['tables']['transformation'][3]['join_type'],
                               on=config['tables']['transformation'][3]['join_column'])

    # Export final denormalized data
    output_file = config['output_file']
    df_denormalized.to_csv(output_file, index=False)
    print(f"{output_file} file created successfully from transformation.py")


# Loading Function
def load_data():
    # Connect to the MySQL database
    db_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="root",
        database="hari2"
    )

    # Load the denormalized data from the CSV file
    df_denormalized = pd.read_csv('denormalized_data.csv')

    # Define the table name for the new table
    new_table_name = 'DenormalizedData'

    # Drop the table if it exists
    cursor = db_connection.cursor()
    drop_table_query = f"DROP TABLE IF EXISTS {new_table_name}"
    cursor.execute(drop_table_query)
    db_connection.commit()




    # Create the new table in the MySQL database
    cursor = db_connection.cursor()
    create_table_query = f"CREATE TABLE {new_table_name} (customer_id INT, first_name VARCHAR(50), full_name VARCHAR(100), order_id INT, order_date VARCHAR(100), year INT, month INT, day INT)"
    cursor.execute(create_table_query)
    db_connection.commit()

    # Insert the denormalized data into the new table
    for _, row in df_denormalized.iterrows():
        customer_id = row['customer_id']
        first_name = row['first_name']
        full_name = row['full_name']
        order_id = row['order_id']
        order_date = row['order_date']
        year = row['year']
        month = row['month']
        day = row['day']

        insert_query = f"INSERT INTO {new_table_name} (customer_id, first_name, full_name, order_id, order_date, year, month, day) VALUES ({customer_id}, '{first_name}', '{full_name}', {order_id}, '{order_date}', {year}, {month}, {day})"
        cursor.execute(insert_query)
        db_connection.commit()

    # Close the database connection
    cursor.close()
    db_connection.close()
    print("Data Loading Completed.")


# Create GUI
window = tk.Tk()
window.title("Data Transformation App")

# Create buttons
extract_button = tk.Button(window, text="Extract Data", command=extract_data)
extract_button.pack()

transform_button = tk.Button(window, text="Transform Data", command=transform_data)
transform_button.pack()

load_button = tk.Button(window, text="Load Data", command=load_data)
load_button.pack()

# Start the GUI event loop
window.mainloop()
