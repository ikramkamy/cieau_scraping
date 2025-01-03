const { exec } = require('child_process');
const mysql = require('mysql'); // or any other database client

// Database credentials
const dbConfig = {
    "host_name": "54.37.69.124",
    "user_name": "lyes",
    "user_password": "xpva9zDctxhn",
    "db_name": "breedgital_cieau_db"
};
const connection = mysql.createConnection({
    "host_name": "54.37.69.124",
    "user_name": "lyes",
    "user_password": "xpva9zDctxhn",
    "db_name": "breedgital_cieau_db"
});

//connect to the database 

connection.connect((err) => {
    if (err) {
        console.error('Error connecting to the database:', err);
        return;
    }
    console.log('Connected to the database.');

    const createTableSQL = `
        CREATE TABLE IF NOT EXISTS plv_uploading_2024_12_02 (
            id INT AUTO_INCREMENT PRIMARY KEY,
            RESEAU VARCHAR(255),
            INSEE VARCHAR(255),
            POSTAL VARCHAR(20),
            DEP VARCHAR(10),
            REG VARCHAR(10),
            COMMUNE VARCHAR(255),
            CODEPLV VARCHAR(50),
            CODEPAR VARCHAR(50),
            PARAMETRE VARCHAR(255),
            UNITE VARCHAR(50),
            RESULTAT VARCHAR(255),
            \`DATEPLV\` DATE,
            \`HEURE\` TIME,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `;

    connection.query(createTableSQL, (error, results) => {
        if (error) {
            console.error('Error creating table:', error);
        } else {
            console.log('Table created successfully:', results);
        }
    });

    connection.end();
});

// Function to run a script
function runScript(scriptPath) {
    return new Promise((resolve, reject) => {
        exec(`node ${scriptPath}`, (error, stdout, stderr) => {
            if (error) {
                console.error(`Error executing script: ${stderr}`);
                return reject(error);
            }
            console.log(`Script output: ${stdout}`);
            resolve();
        });
    });
}
// Function to create a table if it does not exist
function createTableIfNotExists(tableName, createTableSQL) {
    return new Promise((resolve, reject) => {
        const connection = mysql.createConnection(dbConfig);
        connection.connect();

        // connection.query(createTableSQL, (error, results) => {
        //     if (error) {
        //         console.error(`Error creating table ${tableName}: ${error.message}`);
        //         connection.end();
        //         return reject(error);
        //     }
        //     console.log(`Table ${tableName} is ready.`);
        //     connection.end();
        //     resolve();
        // });
    });
}


// Function to upload data to the database
function uploadToDatabase(tableName, filePath) {
    const connection = mysql.createConnection(dbConfig);
    connection.connect();

    const sql = `LOAD DATA INFILE '${filePath}' INTO TABLE ${tableName} FIELDS TERMINATED BY ';' LINES TERMINATED BY '\n'`;
    
    connection.query(sql, (error, results) => {
        if (error) {
            console.error(`Error uploading data: ${error.message}`);
        } else {
            console.log(`Data uploaded to ${tableName}: ${results.affectedRows} rows affected`);
        }
        connection.end();
    });
}

// Main function to orchestrate the scripts
async function main() {
    try {
        // Run the scraping script
        // await runScript('./index.js');

        // Define SQL to create the first table
        const createPLVTableSQL = `
            CREATE TABLE IF NOT EXISTS plv_uploading_2024_12_02 (
              id INT AUTO_INCREMENT PRIMARY KEY,
              RESEAU VARCHAR(255),
              INSEE VARCHAR(255),
              POSTAL VARCHAR(20),
              DEP VARCHAR(10),
              REG VARCHAR(10),
              COMMUNE VARCHAR(255),
              CODEPLV VARCHAR(50),
              CODEPAR VARCHAR(50),
              PARAMETRE VARCHAR(255),
              UNITE VARCHAR(50),
              RESULTAT VARCHAR(255),
              DATEPLV DATE,
              HEURE TIME,
            )
        `;
        await createTableIfNotExists('plv_uploading_2024_12_02', createPLVTableSQL);
        
        // Run the first script to generate the first SQL table
        await runScript('./plv.js');
        await uploadToDatabase('plv_uploading_2024_12_02', './excel/PLV_2024-12-02_upload.csv');

        // Define SQL to create the second table
        const createCnfTableSQL = `
            CREATE TABLE IF NOT EXISTS cnf_uploading_2024_12_02 (
               
                 RESEAU VARCHAR(255),
                 INSEE VARCHAR(255),
                 POSTAL VARCHAR(20),
                 DEP VARCHAR(10),
                 REG VARCHAR(10),
                 LAT DECIMAL(10, 8),  
                 LON DECIMAL(11, 8),  
                 COMMUNE VARCHAR(255),
                 DATE DATE,
                 TOTAL INT DEFAULT 1,  
                 CBL VARCHAR(255),
                 NBL VARCHAR(255),
                 CCL VARCHAR(255),
                 NCL VARCHAR(255),
                 CBR VARCHAR(255),
                 NBR VARCHAR(255),
                 CCR VARCHAR(255),
                 NCR VARCHAR(255),
                 CONCLUSION TEXT,  
                 HEURE TIME,
            )
        `;
        await createTableIfNotExists('cnf_uploading_2024_12_02', createCnfTableSQL);
        
        // Run the second script to generate the second SQL table
        await runScript('./cnf.js');
        await uploadToDatabase('cnf_uploading_2024_12_02', './excel/CNF_2024-12-02.csv');

    } catch (error) {
        console.error(`Error in main process: ${error.message}`);
    }
}

// Execute the main function
main();