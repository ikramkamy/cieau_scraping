const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const XLSX = require('xlsx');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const htmlFolderPath = './html/2025-01-03';

const allData = [];
const maxRowsPerFile = 500000;
// Fonction asynchrone pour lire et traiter les fichiers HTML
async function processFiles() {
    try {
        // Lire tous les fichiers du dossier
        const files = await fs.promises.readdir(htmlFolderPath);

        // Filtrer les fichiers HTML
        const htmlFiles = files.filter(file => path.extname(file).toLowerCase() === '.html');

        let processedFiles = 0; // Compteur pour suivre le nombre de fichiers traités

        // Traiter chaque fichier HTML de manière asynchrone
        for (const file of htmlFiles) {
            const filePath = path.join(htmlFolderPath, file);

            // Lire le fichier HTML
            const html = await fs.promises.readFile(filePath, 'utf8');

            const fileNameWithoutExt = path.parse(file).name;

            // Charger le contenu HTML avec cheerio
            const $ = cheerio.load(html);

            const dataEntry = {
                RESEAU: fileNameWithoutExt.split("_")[1],
                INSEE: fileNameWithoutExt.split("_")[0],
                POSTAL: "",
                DEP: "",
                REG: "",
                LAT: "",
                LON: "",
                COMMUNE: "",
                DATE: "",
                TOTAL: 1,
                CBL: "",
                NBL: "",
                CCL: "",
                NCL: "",
                CBR: "",
                NBR: "",
                CCR: "",
                NCR: "",
                CONCLUSION: "",
                HEURE: ""
            };

            // Première table : récupérer "Date du prélèvement"
            const firstTable = $('table').eq(0);
            firstTable.find('tr').each((_, row) => {
                const key = $(row).find('th').text().trim();
                const value = $(row).find('td').text().trim();

                if (key === 'Date du prélèvement') {
                    const date = value.split('  ')[0];
                    const heure = value.split('  ')[1];
                    const [day, month, year] = date.split('/');
                    var dateFR = `${year}-${month}-${day}`
                    dataEntry['DATE'] = dateFR;
                    dataEntry['HEURE'] = heure;
                }
            });

            // Deuxième table : récupérer les données spécifiques
            const secondTable = $('table').eq(1);
            secondTable.find('tr').each((_, row) => {
                const key = $(row).find('th').text().trim();
                const value = $(row).find('td').text().trim();

                if (key === 'Conclusions sanitaires') {
                    const cleanedValue = value.replace(/[\n\r\t]+/g, ' ').replace(/\s{2,}/g, ' ').trim();
                    dataEntry['CONCLUSION'] = cleanedValue;

                } else if (key === 'Conformité bactériologique') {
                    if (value == 'oui') {
                        dataEntry['CBL'] = "1";
                        dataEntry['NBL'] = "0";
                    } else if (value == 'non') {
                        dataEntry['CBL'] = "0";
                        dataEntry['NBL'] = "1";
                    } else {
                        dataEntry['CBL'] = "";
                        dataEntry['NBL'] = "";
                    }
                } else if (key === 'Conformité physico-chimique') {
                    if (value == 'oui') {
                        dataEntry['CCL'] = "1";
                        dataEntry['NCL'] = "0";
                    } else if (value == 'non') {
                        dataEntry['CCL'] = "0";
                        dataEntry['NCL'] = "1";
                    } else {
                        dataEntry['CCL'] = "";
                        dataEntry['NCL'] = "";
                    }
                } else if (key === 'Respect des références de qualité') {
                    if (value == 'oui') {
                        dataEntry['CBR'] = "1";
                        dataEntry['NBR'] = "0";
                        dataEntry['CCR'] = "1";
                        dataEntry['NCR'] = "0";
                    } else if (value == 'non') {
                        dataEntry['CBR'] = "0";
                        dataEntry['NBR'] = "1";
                        dataEntry['CCR'] = "0";
                        dataEntry['NCR'] = "1";
                    } else {
                        dataEntry['CBR'] = "";
                        dataEntry['NBR'] = "";
                        dataEntry['CCR'] = "";
                        dataEntry['NCR'] = "";
                    }
                }
            });
            if (dataEntry['CBL'] !== "" &&
                dataEntry['NBL'] !== "" && dataEntry['DATE'] !== "" && dataEntry['CONCLUSION'] !== "") {

                allData.push(dataEntry);


            } else {
                //do nothing
            }
      
            allData.sort((a, b) => {
               
                if (a.DATE === b.DATE) {
                    return b.HEURE.localeCompare(a.HEURE); 
                }
                return b.DATE.localeCompare(a.DATE); 
            });

            // Use a Map to keep track of unique INSEE values
            const uniqueData = new Map();

            for (const row of allData) {
                if (!uniqueData.has(row.INSEE)) {
                    // Add only the first occurrence of INSEE (most recent row)
                    uniqueData.set(row.INSEE, row);
                }
            }

            // Convert the Map values back to an array
            const result = Array.from(uniqueData.values());

            // Log the result
            console.log("Filtered data:", result);

            //console.log('dataEntry', dataEntry)
            console.log(processedFiles)
            processedFiles++;

            // Si tous les fichiers ont été traités, générer l'Excel
            // console.log("processedFiles",processedFiles)
            // console.log('htmlFiles.length',htmlFiles.length)
            if (processedFiles === htmlFiles.length) {
                // Générer un fichier Excel avec les données

                const outputFileName = `CNF_${new Date().toISOString()}.xlsx`;
                const outputFilePath = path.join('./excel', `CNF_${new Date().toISOString().split("T")[0]}.xlsx`);
                const outputFilePathCsv = path.join(`./csv/CNF_${new Date().toISOString().split("T")[0]}.csv`);
                if (!fs.existsSync('./excel')) {
                    fs.mkdirSync('./excel');
                }
                if (!fs.existsSync('./csv')) {
                    fs.mkdirSync('./csv');
                }

                // Convertir les données en format approprié pour XLSX
                const worksheet = XLSX.utils.json_to_sheet(allData);
                const workbook = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(workbook, worksheet, "data");

                // Écrire le fichier Excel
                XLSX.writeFile(workbook, outputFilePath);
                console.log(`Fichier Excel exporté sous ${outputFilePath}`);
                // Create CSV writer
                const csvWriter = createCsvWriter({
                    path: outputFilePathCsv,
                    header: [
                        { id: 'RESEAU', title: 'RESEAU' },
                        { id: 'INSEE', title: 'INSEE' },
                        { id: 'POSTAL', title: 'POSTAL' },
                        { id: 'DEP', title: 'DEP' },
                        { id: 'REG', title: 'REG' },
                        { id: 'LAT', title: 'LAT' },
                        { id: 'LON', title: 'LON' },
                        { id: 'COMMUNE', title: 'COMMUNE' },
                        { id: 'DATE', title: 'DATE' },
                        { id: 'TOTAL', title: 'TOTAL' },
                        { id: 'CBL', title: 'CBL' },
                        { id: 'NBL', title: 'NBL' },
                        { id: 'CCL', title: 'CCL' },
                        { id: 'NCL', title: 'NCL' },
                        { id: 'CBR', title: 'CBR' },
                        { id: 'NBR', title: 'NBR' },
                        { id: 'CCR', title: 'CCR' },
                        { id: 'NCR', title: 'NCR' },
                        { id: 'CONCLUSION', title: 'CONCLUSION' },
                        //keep heur for csv file for verification and exclude it while inserting in sql table
                        { id: 'HEURE', title: 'HEURE' },
                    ]
                });
                // Write data to CSV
                const chunk =  result.splice(0, maxRowsPerFile);
                await csvWriter.writeRecords(chunk);
                console.log(`Fichier CSV exporté sous ${outputFilePathCsv}`);

            }
        }
    } catch (err) {
        console.error('Erreur:', err);
    }
}

// Appeler la fonction pour traiter les fichiers
processFiles();
