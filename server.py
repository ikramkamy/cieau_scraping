import mysql.connector
from mysql.connector import Error
import pandas as pd


# Function to create a connection to the database
def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print(f"Connection to {db_name} DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

# Function to fetch data from the local database
# def fetch_data_from_local(connection, query, limit):
#     cursor = connection.cursor()
#     cursor.execute(query, (limit,))
#     result = cursor.fetchall()
#     cursor.close()
#     return result

# Function to insert data into the remote database
# def insert_data_to_remote(connection, query, data):
#     cursor = connection.cursor()
#     cursor.executemany(query, data)
#     connection.commit()
#     cursor.close()
# Function to read CSV and insert into remote database
def insert_csv_to_table(csv_file_path, table_name, remote_connection, chunksize=1000):
    # Read CSV file in chunks for efficiency (if large files)
    for chunk in pd.read_csv(csv_file_path, chunksize=chunksize):
        # Prepare the SQL INSERT query based on the columns
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        columns = ', '.join(chunk.columns)
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
        # Convert DataFrame rows to list of tuples
        data = [tuple(row) for row in chunk.to_numpy()]
        
        # Execute the insert query
        cursor = remote_connection.cursor()
        cursor.executemany(query, data)
        remote_connection.commit()
        cursor.close()

# Example usage
def main():
    # Connect to remote database
    remote_connection = mysql.connector.connect(
        host="54.37.69.124",
        user="lyes",
        password="xpva9zDctxhn",
        database="breedgital_cieau_db",
)

    # Define CSV file paths and corresponding table names
    csv_files_and_tables = [
        ("./excel/PLV_2024-12-02_upload.csv", "plv_uploading_2024_12_02"),
        ("./excel/CNF_2024-12-02.csv", "cnf_uploading_2024_12_02"),
    ]

    # Insert each CSV file into the corresponding table
    for csv_file, table in csv_files_and_tables:
        insert_csv_to_table(csv_file, table, remote_connection)

    # Close remote connection
    remote_connection.close()

if __name__ == "__main__":
    main()
# # Function to update the 'transferred' status in the local database
# def update_transferred_status(connection, ids):
#     cursor = connection.cursor()
#     update_query = "UPDATE dis_upload_cnf_data SET transferred = 1 WHERE ID IN (%s)" % ','.join(['%s'] * len(ids))
#     cursor.execute(update_query, ids)
#     connection.commit()
#     cursor.close()

# Function to log the transfer in the tracking table
# def log_transfer(connection, records_transferred):
#     cursor = connection.cursor()
#     log_query = """
#     INSERT INTO transfer_tracking (records_transferred, transfer_time)
#     VALUES (%s, NOW())
#     """
#     cursor.execute(log_query, (records_transferred,))
#     connection.commit()
#     cursor.close()

# Connection details
local_db_details = {
    "host_name": "localhost",
    "user_name": "root",
    "user_password": "",
    "db_name": "cieau"
}

remote_db_details = {
    "host_name": "54.37.69.124",
    "user_name": "lyes",
    "user_password": "xpva9zDctxhn",
    "db_name": "breedgital_cieau_db"
}

# Queries
# fetch_query = """
# SELECT `ID`, `INSEE`, `POSTAL`, `COMMUNE`, `CODEPLV`, `CODEPAR`, `PARAMETRE`, `UNITE`, `RESULTAT`, `DATEPLV`, `DATEMAJ`, `DATEALS`, `installation`, `service public de distribution`, `responsable de distribution`, `maitre douvrage`, `conclusions sanitaires`, `conformite bacteriologique`, `conformite physico chimique`, `respect des references de qualite`, `COMMUNE_2`, `DEP`, `REG`, `index_region`, `index_departement`, `index_commune`, `index_reseau`
# FROM dis_plv_2022
# WHERE transferred = 0 LIMIT %s
# """

# insert_query = """
# INSERT IGNORE INTO dis_plv_2022 (
#     `ID`, `INSEE`, `POSTAL`, `COMMUNE`, `CODEPLV`, `CODEPAR`, `PARAMETRE`, `UNITE`, `RESULTAT`, `DATEPLV`, `DATEMAJ`, `DATEALS`, `installation`, `service public de distribution`, `responsable de distribution`, `maitre douvrage`, `conclusions sanitaires`, `conformite bacteriologique`, `conformite physico chimique`, `respect des references de qualite`, `COMMUNE_2`, `DEP`, `REG`, `index_region`, `index_departement`, `index_commune`, `index_reseau`
# ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# """

# Create connections
# local_connection = create_connection(
#     local_db_details["host_name"],
#     local_db_details["user_name"],
#     local_db_details["user_password"],
#     local_db_details["db_name"]
# )

remote_connection = create_connection(
    remote_db_details["host_name"],
    remote_db_details["user_name"],
    remote_db_details["user_password"],
    remote_db_details["db_name"]
)

# Initialize variables for batch processing
# batch_size = 1000

# counter = 1
# while True:
#     print(counter)
#     # Fetch data from local database in batches
#     data = fetch_data_from_local(local_connection, fetch_query, batch_size)
#     if not data:
#         break  # Exit the loop if no more data is fetched
#     # Extract the IDs of the rows to be transferred
#     ids = [row[0] for row in data]
#     # Insert data into remote database
#     insert_data_to_remote(remote_connection, insert_query, data)
#     # Mark rows as transferred in the local database
#     update_transferred_status(local_connection, ids)
#     # Log the transfer details
#     log_transfer(remote_connection, len(data))
#     counter += 1

# # Close connections
# local_connection.close()
# remote_connection.close()