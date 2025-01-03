import mysql.connector
from mysql.connector import Error
import pandas as pd


def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host="54.37.69.124",
            user="lyes",
            passwd="xpva9zDctxhn",
            database="breedgital_cieau_db"
        )
        print(f"Connection to {db_name} DB successful")
    except Error as e:
        print(f"The error '{e}' occurred")
    return connection

remote_db_details = {
    "host_name": "54.37.69.124",
    "user_name": "lyes",
    "user_password": "xpva9zDctxhn",
    "db_name": "breedgital_cieau_db"
}
remote_connection = create_connection(
    remote_db_details["host_name"],
    remote_db_details["user_name"],
    remote_db_details["user_password"],
    remote_db_details["db_name"]
)

def create_table(connection, table_name):
    cursor = connection.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {"plv_uploading_2024_12_02"} (
         id INT AUTO_INCREMENT PRIMARY KEY
    )
    """
    try:
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created successfully.")
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()

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

# # Example usage
def main():
    # Connect to remote database
    remote_connection = create_connection(
    remote_db_details["host_name"],
    remote_db_details["user_name"],
    remote_db_details["user_password"],
    remote_db_details["db_name"]
    )
# Create a new table for testing
    create_table(remote_connection, "test_table")
# Define CSV file paths and corresponding table names
    # csv_files_and_tables = [
    #     ("./excel/PLV_2024-12-02_upload.csv", "plv_uploading_2024_12_02"),
    #     ("./excel/CNF_2024-12-02.csv", "cnf_uploading_2024_12_02"),
    # ]

    # # Insert each CSV file into the corresponding table
    # for csv_file, table in csv_files_and_tables:
    #     insert_csv_to_table(csv_file, table, remote_connection)

    # Close remote connection
    remote_connection.close()

if __name__ == "__main__":
    main()





CREATE TABLE NEW_prod_dec_02_cnf SELECT  
    com_2024_nov.ID, 
    com_2024_nov.INSEE,
    com_2024_nov.POSTAL,
    com_2024_nov.DEP,
    com_2024_nov.REG,
    com_2024_nov.LAT,
    com_2024_nov.LON,
    com_2024_nov.LIBELLE AS COMMUNE,
    temp_keep.DATE,
    temp_keep.TOTAL,
    temp_keep.CBL,
    temp_keep.NBL,
    temp_keep.CCL,
    temp_keep.NCL,
    temp_keep.CBR,
    temp_keep.NBR,
    temp_keep.CCR,
    temp_keep.NCR,
    temp_keep.CONCLUSION
FROM 
    com_2024_nov
INNER JOIN 
    temp_keep 
ON 
    temp_keep.INSEE = com_2024_nov.INSEE_DATA;
