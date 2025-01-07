import mysql.connector
from mysql.connector import Error
import pandas as pd
from datetime import datetime
import os
import subprocess


today_date = datetime.now().strftime("%Y_%m_%d")
today_date_csv_file_name = datetime.now().strftime("%Y-%m-%d")

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

def Start_scraping():
    print('we are starting scraping')
    node_script = os.path.join(os.getcwd(), 'index.js') 
    try:
        result = subprocess.run(['node', node_script], check=True, capture_output=True, text=True)
        print("Node.js scraping script executed successfully.")
        print("Output:", result.stdout)
    except subprocess.CalledProcessError as e:
        print("Error executing scraping Node.js script:", e.stderr)
        return None

def create_csv_from_scraping(scraping_folder):
    # Define the path to the Node.js script
    node_script = os.path.join(os.getcwd(), 'plv.js') 
    node_script_cnf = os.path.join(os.getcwd(), 'cnf.js')
    # Trigger the Node.js script
    try:
        # result = subprocess.run(['node', node_script, scraping_folder], check=True, capture_output=True, text=True)
        # print("Output:", result.stdout)
        # print("Node.js plv script executed successfully.")
        result_cnf = subprocess.run(['node', node_script_cnf, scraping_folder], 
                                    check=True, capture_output=True, text=True)
        print("Node.js cnf script executed successfully.")
        print("Output:", result_cnf.stdout)
    except subprocess.CalledProcessError as e:
        print("Error executing Node.js script:", e.stderr)
        return None

    # Define the path to the generated CSV file
    #csv_file_path = os.path.join(scraping_folder, 'plv.csv')  # Adjust the filename as necessary
    csv_file_path_cnf = os.path.join(scraping_folder, 'cnf.csv')  # Adjust the filename as necessary
    # Read the CSV file
    try:
        #df = pd.read_csv(csv_file_path)
        #print("CSV file read successfully.")
        df_cnf = pd.read_csv(csv_file_path_cnf)
        print("CSV  for cnf file read successfully.")  
        return df_cnf
    except FileNotFoundError:
        print("CSV file not found.")
        return None
    except Exception as e:
        print("Error reading CSV file:", e)
        return None

# Function to read CSV and insert into remote database
def insert_csv_to_table(csv_file_path, table_name, remote_connection, chunksize=1000):
   
    for chunk in pd.read_csv(csv_file_path, sep=',', chunksize=chunksize):
        chunk.fillna('', inplace=True)
        scraping_plv_table = f"plv_scraping_results_{today_date}"
        # Prepare the SQL INSERT query based on the columns
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        columns = ', '.join(chunk.columns)
        print("we are adding data to :",scraping_plv_table)
        query = f"INSERT INTO {scraping_plv_table} ({columns}) VALUES ({placeholders})"
        # Convert DataFrame rows to list of tuples
        data = [tuple(row) for row in chunk.to_numpy()]
        cursor = remote_connection.cursor()
        cursor.executemany(query, data)
        remote_connection.commit()

        print("we are adding data from :",scraping_plv_table)
        query_prod = f"""
        INSERT INTO dis_res_12_mois 
        SELECT 
            dis_par_2022.REFERENCE AS REF,
            dis_par_2022.SISE,
            dis_par_2022.CODE,
            {scraping_plv_table}.RESULTAT,
            {scraping_plv_table}.DATEPLV AS DATE,
            {scraping_plv_table}.INSEE
        FROM {scraping_plv_table} 
        INNER JOIN dis_par_2022 ON dis_par_2022.LIBELLE = {scraping_plv_table}.PARAMETRE
        """
     
        
        # Execute the insert query
        cursor = remote_connection.cursor()
        cursor.execute(query_prod)

        remote_connection.commit()
        cursor.close()
# creating plv scraping table


def create_table_plv_with_data(connection):
    cursor = connection.cursor()
    new_table_name = f"plv_scraping_results_{today_date}"
    new_table_name_cnf = f"cnf_scraping_results_{today_date}"
    csv_file = f"./csv/PLV_{today_date_csv_file_name}.csv"
    csv_file_cnf = f"./csv/CNF_{today_date_csv_file_name}.csv"
    # create_table_query = f"""
    # CREATE TABLE `prod_res_12_mois_scraping` LIKE `plv_uploading_2024_12_02`;
    # """
    try:
        #cursor.execute(create_table_query)
        #print(f"Table '{new_table_name}' created successfully.")
        print(f"Table 'prod_res_12_mois_scraping' created successfully.")
        # Define CSV file paths and corresponding table names
        csv_files_and_tables = [(f"./csv/PLV_scraping_{today_date_csv_file_name}.csv",
                                  "prod_res_12_mois_scraping")]
        csv_files_and_tables_cnf = [(csv_file_cnf,
                                  "prod_cnf")]
     
        # for csv_file, table in csv_files_and_tables:
        #     insert_csv_to_table(csv_file, table, connection) 
        for csv_file, table in csv_files_and_tables_cnf:
            insert_csv_to_table(csv_file_cnf, table, connection) 
    except Error as e:
        print(f"The error '{e}' occurred")
    finally:
        cursor.close()
### CNF ###
def insert_csv_CNF_to_table(csv_file_path, remote_connection, chunksize=1000):
    for chunk in pd.read_csv(csv_file_path, sep=',', chunksize=chunksize):
        chunk.fillna(0, inplace=True)

        scraping_CNF_table = f"cnf_scraping_results"
        # Prepare the SQL INSERT query based on the columns
        placeholders = ', '.join(['%s'] * len(chunk.columns))
        columns = ', '.join(chunk.columns)
        print("We are adding data to:", scraping_CNF_table)

        cursor = remote_connection.cursor()
 

        # Prepare the INSERT query with ON DUPLICATE KEY UPDATE
        query = f"""    
        INSERT INTO {scraping_CNF_table} ({columns}) VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE 
        {', '.join([f'{col}=VALUES({col})' for col in chunk.columns])}
        """
        
        # Convert DataFrame rows to list of tuples
        data = [tuple(row) for row in chunk.to_numpy()]

        # Execute the insert query
        cursor.executemany(query, data)
        remote_connection.commit()

        print("We are adding CNF data from:", scraping_CNF_table)

        query_prod_cnf = f"""
        UPDATE dis_cnf_prod AS target
        JOIN com_2024 AS source
        ON target.INSEE = source.INSEE
        JOIN cnf_scraping_results AS cnf_source
        ON cnf_source.INSEE = source.INSEE_DATA
        SET 
            target.DATE = cnf_source.DATE,
            target.TOTAL = cnf_source.TOTAL,
            target.CBL = cnf_source.CBL,
            target.NBL = cnf_source.NBL,
            target.CCL = cnf_source.CCL,
            target.NCL = cnf_source.NCL,
            target.CBR = cnf_source.CBR,
            target.NBR = cnf_source.NBR,
            target.CCR = cnf_source.CCR,
            target.NCR = cnf_source.NCR,
            target.CONCLUSION = cnf_source.CONCLUSION
        """

        # Execute the update query
        cursor.execute(query_prod_cnf)
        remote_connection.commit()
        cursor.close()


def main():
    # Connect to remote database
    remote_connection = create_connection(
    remote_db_details["host_name"],
    remote_db_details["user_name"],
    remote_db_details["user_password"],
    remote_db_details["db_name"]
    )

    #Start_scraping()
    #create_csv_from_scraping("/html/2025-01-03")
    print(f"/csv/CNF_{today_date_csv_file_name}.csv")
    insert_csv_CNF_to_table("csv/CNF_2025-01-06.csv",remote_connection)
    
    #create_table_plv_with_data(remote_connection)
    remote_connection.close()

if __name__ == "__main__":
    main()