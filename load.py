import pandas as pd
import mysql.connector

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
print('The Denormalize datas are Loaded To Your Database Sucessfully')
db_connection.close()
