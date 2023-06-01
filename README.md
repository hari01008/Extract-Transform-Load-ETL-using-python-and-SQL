# Extract-Transform-Load-ETL-using-python-and-SQL
This Project is about ETL concept (Extract Tranform And Load) Here we extract data from database and transform with the help of config.json file and transform the datas with the given operations and load to the database with python and mysql




ABOUT :
1)	Extract.py –  Used to Extract the normalized table from the database and it creates .csv file and store the tables on that csv file.
2)	Transform.py- Used to transform data from database with these transformations
1)Date format change (mm-dd-yyyy)to(yyyy-mm-dd)
2)concatenate data from two column to one column
3)split string from one column to two column
4)drop one or more columns
5)join two or more tables to make the denormalized table

Config.json used for the transformation

3)	Load.py – Used to Load denormalized table to database





HOW TO RUN THE PROJECT :

•	First create a table and column in Mysql “Mysqlworkbench” (SQL.docx)file given in this folder
•	Then open the all files or open the project folder in vscode and install all required libraries , The libraries are given below this document.
•	MODIFY THE CODE WITH YOUR DATABASE CREDIENTIALS ….(MUST)
•	You can run by :  python extract.py .
•	It creates new folder called extracted_data and the .csv files are presented on that folder
•	Then you need to run : python transformation.py
•	Here the data transformations are done and its create a denormalized data on the same folder
•	Then you need to run : python load.py
•	Here the denormalized datas are load to the database
•	YOU CAN RUN THESE USING “ GUI ” using : python app.py : it contains all operations in GUI








REQUIRED LIBRARIES:
•	Pandas
•	Sqlalchemy
•	Mysql.connector.python
•	Tkinter
•	Filedialog
•	subprocess


