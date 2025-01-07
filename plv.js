const fs = require('fs');
const path = require('path');
const cheerio = require('cheerio');
const XLSX = require('xlsx');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
// Chemin vers le dossier contenant les fichiers HTML
//const htmlFolderPath = './html/2024-12-02'; // Remplacez par le chemin de votre dossier
const htmlFolderPath = './html/2025-01-03';
// Tableau pour stocker toutes les données à inclure dans l'Excel final
const allData = [];
const maxRowsPerFile = 500000;
const selectedParam = ["Calcium", "Magnésium", "Sodium", "Potassium", "Sulfates", "Hydrogénocarbonates", "pH", "pH *", "Titre hydrotimétrique", "Chlore total *", "Chlore total", "Fluorures mg/L", "Total des pesticides analysés", "Nitrates (en NO3)"]

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
                COMMUNE: "",
                CODEPLV: "",
                CODEPAR: "",
                PARAMETRE: "",
                UNITE: "",
                RESULTAT: "",
                DATEPLV: "",
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
                    dataEntry['DATEPLV'] = dateFR;
                    dataEntry['HEURE'] = heure;
                }
            });

            // Deuxième table : récupérer les données spécifiques
            const secondTable = $('table').eq(2); // Assurez-vous que vous accédez à la bonne table (index 1 pour la deuxième table)
            secondTable.find('tr').each((_, row) => {
                const columns = $(row).find('td'); // Sélectionner toutes les colonnes de la ligne
                if (columns.length >= 2) { // Vérifier qu'il y a au moins deux colonnes
                    const parametre = $(columns[0]).text().trim(); // Première colonne
                    const resultat = $(columns[1]).text().trim();  // Deuxième colonne


                    // Créez une copie de dataEntry pour chaque ligne
                    var dataEntryParam = { ...dataEntry }; // Copie superficielle de dataEntry
                    dataEntryParam['PARAMETRE'] = parametre;
                    dataEntryParam['RESULTAT'] = resultat.split(" ")[0];


                    // Eliminate "*" from PARAMETRE
                    dataEntryParam['PARAMETRE'] = dataEntryParam['PARAMETRE'].replace(/\*/g, '').trim();

                    // Eliminate ">" from RESULTAT
                    dataEntryParam['RESULTAT'] = dataEntryParam['RESULTAT'].replace(/>/g, '').trim();

                    // Replace any value containing "<" with 0 in RESULTAT
                    if (dataEntryParam['RESULTAT'].includes('<')) {
                        dataEntryParam['RESULTAT'] = '0';
                    }

                    if (selectedParam.indexOf(parametre) != -1) {
                        //console.log('dataEntryParam',dataEntryParam)
                        allData.push(dataEntryParam); // Ajoutez l'objet à allData
                    }
                }
            });

            console.log("processed files for plv creation",processedFiles)
            processedFiles++;

            // Si tous les fichiers ont été traités, générer les fichiers Excel
            if (processedFiles === htmlFiles.length) {
                // Split data into chunks
                let chunkIndex = 1;
                while (allData.length > 0) {
                    const chunk = allData.splice(0, maxRowsPerFile); // Extraire un morceau de 200 000 lignes
                    const outputFileName = `PLV_${new Date().toISOString().split("T")[0]}_${chunkIndex}.xlsx`;
                    console.log("file name", `./csv/PLV_${new Date().toISOString().split("T")[0]}.csv`)
                    const outputFilePath = path.join('./excel', "PLV_2024-12-03_ready.xlsx");
                    const outputFilePathCsv = path.join(`./csv/PLV_verify${new Date().toISOString().split("T")[0]}.csv`);

                    if (!fs.existsSync('./excel')) {
                        fs.mkdirSync('./excel');
                    }

                    // Convertir les données en format approprié pour XLSX
                    const worksheet = XLSX.utils.json_to_sheet(chunk);
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
                            { id: 'COMMUNE', title: 'COMMUNE' },
                            { id: 'CODEPLV', title: 'CODEPLV' },
                            { id: 'CODEPAR', title: 'CODEPAR' },
                            { id: 'PARAMETRE', title: 'PARAMETRE' },
                            { id: 'UNITE', title: 'UNITE' },
                            { id: 'RESULTAT', title: 'RESULTAT' },
                            { id: 'DATEPLV', title: 'DATEPLV' },
                            //{ id: 'HEURE', title: 'HEURE' }
                        ]
                    });

                    // Write data to CSV
                    await csvWriter.writeRecords(chunk);
                    console.log(`Fichier CSV exporté sous ${outputFilePathCsv}`);
                    chunkIndex++; // Incrémenter le compteur de fichier
                }
            }
        }
    } catch (err) {
        console.error('Erreur:', err);
    }


}

// Appeler la fonction pour traiter les fichiers
processFiles();
