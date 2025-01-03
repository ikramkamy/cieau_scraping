import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime

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

# Function to read CSV and insert into remote database
def insert_csv_to_table(csv_file_path, table_name, remote_connection, chunksize=1000):
    #Read CSV file in chunks for efficiency (if large files)
    for chunk in pd.read_csv(csv_file_path, sep=',', chunksize=chunksize):
        chunk.fillna('default_value', inplace=True)
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
# creating plv scraping table
def create_table_plv(connection):
    cursor = connection.cursor()
    # Get today's date in YYYY_MM_DD format
    today_date = datetime.now().strftime("%Y_%m_%d")
    today_date_csv_file_name=datetime.now().strftime("%Y-%m-%d")
    new_table_name = f"plv_scraping_results_{today_date}"
    csv_file_name=f"PLV_{today_date_csv_file_name}.csv"
    
    create_table_query = f"""
    CREATE TABLE `{new_table_name}` LIKE `plv_uploading_2024_12_02`;
    """
# print(`./csv/PLV_${new Date().toISOString().split("T")[0]}.csv`)
    print("today_date",f"./csv/PLV_{today_date_csv_file_name}.csv")
    try:
        cursor.execute(create_table_query)
        print(f"Table '{new_table_name}' created successfully.")

        # Define CSV file paths and corresponding table names
        csv_files_and_tables = [(f"./csv/PLV_{today_date_csv_file_name}.csv", new_table_name)]

        # Insert each CSV file into the corresponding table
        for csv_file, table in csv_files_and_tables:
            insert_csv_to_table(csv_file, table, connection)  # Use 'connection' instead of 'remote_connection'
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()
# creating CNF table 
# def create_table_cnf(connection, table_name):
#     cursor = connection.cursor()
#      # Get today's date in YYYYMMDD format
#     today_date = datetime.now().strftime("%Y_%m_%d")
#     new_table_name = f"cnf_scraping_results_{today_date}"

#     create_table_query = f"""
#     CREATE TABLE `{new_table_name}` LIKE `cnf_uploading_2024_12_02`;
#     """
#     try:
#         cursor.execute(create_table_query)
#         print(f"Table '{table_name}' created successfully.")
#     # Define CSV file paths and corresponding table names
#     csv_files_and_tables = [("./excel/CNF_2024-12-02.csv","new_table_name")]

#     # # Insert each CSV file into the corresponding table
#     for csv_file, table in csv_files_and_tables:
#         insert_csv_to_table(csv_file, new_table_name, remote_connection)
#     except Error as e:
#         print(f"The error '{e}' occurred")
#     finally:
#         cursor.close()


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
    create_table_plv(remote_connection)
    # create_table_cnf(remote_connection, "cnf table")


    # Close remote connection
    remote_connection.close()

if __name__ == "__main__":
    main()