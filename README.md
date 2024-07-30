# database_migrator
MySQL to PostgreSQL Migration Tool
This Python script facilitates the migration of data from a MySQL table to a PostgreSQL table. It transfers the data in batches, ensuring that large datasets can be efficiently migrated without overwhelming system resources.

Prerequisites
Python 3.x
MySQL
PostgreSQL
Python libraries:
pymysql
psycopg2
You can install the required Python libraries using pip:

sh pip install pymysql psycopg2

Usage
Specify the Table Name: Update the table_name variable with the name of the table you want to migrate.

JSON File Path: Ensure the json_file_path points to the JSON file used for tracking the last inserted record. If the file does not exist, create it with the following initial content: json { "last_inserted_record": 0 }

Run the Script: Execute the script using Python. sh python migrate.py

Customization
Batch Size: Adjust the batch_size variable to control the number of rows fetched from the MySQL table in each batch.
Conflict Handling: The script uses ON CONFLICT DO NOTHING to handle conflicts in the PostgreSQL table. You can modify this behavior as needed.
