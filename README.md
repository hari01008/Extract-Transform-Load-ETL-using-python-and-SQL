# Extract Transform Load (ETL) using Python and SQL

Welcome to the Extract-Transform-Load (ETL) project! This project focuses on the ETL concept, where data is extracted from a database, transformed using specified operations from a `config.json` file, and loaded back into the database using Python and MySQL.

## About the Project

The project encompasses the following main components:

- **Extract.py:** This script extracts data from a database and creates normalized tables, storing the extracted data in CSV files.

- **Transform.py:** In this step, data is transformed using a series of operations defined in the `config.json` file. Operations include date format change, column concatenation, string splitting, column dropping, and table joins for denormalization.

- **Load.py:** The transformed and denormalized data is loaded back into the database.

## How to Run the Project

1. Create the required tables and columns in MySQL as described in the provided `SQL.docx` file.

2. Open the project folder in an IDE like VSCode and ensure that you have the required libraries installed. The list of required libraries is provided below.

3. **Modify Database Credentials:** Update the code with your database credentials before running the scripts.

4. Run the following scripts in sequence:
   - `python extract.py` to create CSV files in a folder named `extracted_data`.
   - `python transformation.py` to perform data transformations and create denormalized data.
   - `python load.py` to load the denormalized data back into the database.

5. Alternatively, you can use the GUI by running: `python app.py`.

## Required Libraries

Ensure you have the following libraries installed:
- Pandas
- Sqlalchemy
- Mysql.connector.python
- Tkinter
- Filedialog
- Subprocess

## Contact

For any questions or further details, feel free to contact me at haricse0808@gmail.com.
