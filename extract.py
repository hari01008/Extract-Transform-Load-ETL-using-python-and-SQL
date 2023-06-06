import pandas as pd
import mysql.connector
import os

# Mysql Connection :
db_connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="root",
    database="hari2"
)

# Define query to retrieve all table names in database
table_query = "SHOW TABLES"

# Execute the query and fetch the data
cursor = db_connection.cursor()
cursor.execute(table_query)
tables = cursor.fetchall()

# Create a folder named "extracted_data" if it doesn't exist
folder_path = "extracted_data"
if not os.path.exists(folder_path):
    os.mkdir(folder_path)
    print(f"Created folder '{folder_path}'")

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
    file_path = os.path.join(folder_path, file_name)
    df.to_csv(file_path, index=False)
    print(f"The Extraction Process for table {table_name} was done successfully and the output stored as {file_path}")

# Close the database connection
cursor.close()
db_connection.close()
