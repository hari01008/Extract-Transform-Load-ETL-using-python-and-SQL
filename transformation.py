import pandas as pd
from sqlalchemy import create_engine
import json

# Load the JSON config file
with open('config.json') as f:
    config = json.load(f)

# Connect to the MySQL database using SQLAlchemy
db_connection = create_engine('mysql+mysqlconnector://root:root@localhost/hari2')
# Load the data from the tables specified in the config
df_customers = pd.read_sql(f"SELECT * FROM {config['tables']['transformation'][0]['source_table']}", db_connection)
df_orders = pd.read_sql(f"SELECT * FROM {config['tables']['transformation'][1]['source_table']}", db_connection)

# Perform transformations specified in the config
# 1. Change date format
df_orders[config['tables']['transformation'][1]['target_column'][0]] = pd.to_datetime(df_orders[config['tables']['transformation'][1]['source_column']], format=config['tables']['transformation'][1]['date_format']).dt.year
df_orders[config['tables']['transformation'][1]['target_column'][1]] = pd.to_datetime(df_orders[config['tables']['transformation'][1]['source_column']], format=config['tables']['transformation'][1]['date_format']).dt.month
df_orders[config['tables']['transformation'][1]['target_column'][2]] = pd.to_datetime(df_orders[config['tables']['transformation'][1]['source_column']], format=config['tables']['transformation'][1]['date_format']).dt.day

# 2. Concatenate columns
df_customers[config['tables']['transformation'][0]['target_column']] = df_customers[config['tables']['transformation'][0]['source_column'][0]] + ' ' + df_customers[config['tables']['transformation'][0]['source_column'][1]]

# 3. Drop columns
df_customers.drop(columns=config['tables']['transformation'][2]['drop_columns'], inplace=True)

# 4. Join tables
df_denormalized = pd.merge(df_customers, df_orders, how=config['tables']['transformation'][3]['join_type'], on=config['tables']['transformation'][3]['join_column'])

# Export final denormalized data
df_denormalized.to_csv(config['output_file'], index=False)
print(f'{config["output_file"]} file created successfully from transformation.py')
