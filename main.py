import pymysql
import psycopg2 
import json
import time

# TODO Change the opts according the required table 
mydb_opts = {
    'user': 'username',
    'password': 'password',
    'host': 'localhost',
    'port': 3306,
    'database': 'db_name'
}

pgdb_opts = {
    'user': 'username',
    'password': 'password',
    'host': 'localhost',
    'port': 5432,
    'database': 'db_name'
}

# TODO Specify the table name
table_name = 'table_name'
json_file_path = './sync_det.json'

# Number of rows to be fetched from MYSQL table in a single query
batch_size = 10000

program_start_time = time.time()

# Create connections with databases
mydb = pymysql.connect(**mydb_opts)
mycursor = mydb.cursor()

pgdb = psycopg2.connect(**pgdb_opts)
pgcursor = pgdb.cursor()

# Fetch total number of rows in the MYSQL table
sql = f'SELECT COUNT(*) FROM {table_name}'
mycursor.execute(sql)
result = mycursor.fetchall()
total_records = result[0][0]


# Fetch last inserted row in postgres table from JSON file
record_no = 0
json_object = {}

with open(json_file_path, 'r') as jsonfile:
    json_object = json.load(jsonfile)
    record_no = json_object['last_inserted_record']
    last_inserted_record = record_no

batch_insert_sql = f"INSERT INTO {table_name} VALUES "
batch_values = []

# Migrate MYSQL table data to postgres table
while(record_no < total_records):
    # Fetch next record in the MYSQL table
    sql = f'SELECT * from {table_name} LIMIT {batch_size} OFFSET {record_no}'
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    
    for row in myresult:
        #TODO Format values to be inserted in postgres table before appending it to batch_values
        batch_values.append(str(tuple([data for data in row])))
    
    batch_insert_sql += ",".join(batch_values) + " ON CONFLICT DO NOTHING"
    pgcursor.execute(batch_insert_sql)
    batch_values = []
    batch_insert_sql = f"INSERT INTO {table_name} VALUES "

    # Commit the changes done to the postgres table
    pgdb.commit()

    sql = f'SELECT COUNT(*) FROM {table_name}'
    pgcursor.execute(sql)
    pgresult = pgcursor.fetchall()
    total_pgrecords = pgresult[0][0]

    record_no = total_pgrecords;
    
    # Update the JSON file with the last inserted row
    with open(json_file_path, 'w') as jsonfile:
        json_object['last_inserted_record'] = record_no
        json_obj = json.dumps(json_object, indent=4)
        jsonfile.write(json_obj)
    
    print(f"{record_no} rows migrated out of {total_records}")
    print(f"Remaining: {total_records - record_no}")


# Close connections
pgdb.close()
mydb.close()

program_end_time = time.time()
execution_time = program_end_time - program_start_time
print(f"Time taken for migrating {record_no - last_inserted_record} records: {execution_time // 3600:.0f} hrs {execution_time // 60:.0f} min {execution_time:.2f} sec")