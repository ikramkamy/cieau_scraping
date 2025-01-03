const fs = require('fs');
const path = require('path');
const { JSDOM } = require('jsdom');
const XLSX = require('xlsx');

// Chemin vers le dossier contenant les fichiers HTML
const htmlFolderPath = './html'; // Remplacez par le chemin de votre dossier

// Tableau pour stocker toutes les données à inclure dans l'Excel final
const allData = [];

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

            // Charger le contenu HTML avec JSDOM
            const dom = new JSDOM(html);
            const document = dom.window.document;

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
            const firstTable = document.querySelectorAll('table')[0];
            if (firstTable) {
                const rows = firstTable.querySelectorAll('tr');
                rows.forEach(row => {
                    const key = row.querySelector('th')?.textContent.trim();
                    const value = row.querySelector('td')?.textContent.trim();

                    if (key === 'Date du prélèvement') {
                        const [date, heure] = value.split('  ');
                        const [day, month, year] = date.split('/');
                        dataEntry['DATE'] = `${year}-${month}-${day}`;
                        dataEntry['HEURE'] = heure;
                    }
                });
            }

            // Deuxième table : récupérer les données spécifiques
            const secondTable = document.querySelectorAll('table')[1];
            if (secondTable) {
                const rows = secondTable.querySelectorAll('tr');
                rows.forEach(row => {
                    const key = row.querySelector('th')?.textContent.trim();
                    const value = row.querySelector('td')?.textContent.trim();

                    if (key === 'Conclusions sanitaires') {
                        dataEntry['CONCLUSION'] = value;
                    } else if (key === 'Conformité bactériologique') {
                        if (value === 'oui') {
                            dataEntry['CBL'] = "1";
                            dataEntry['NBL'] = "0";
                        } else if (value === 'non') {
                            dataEntry['CBL'] = "0";
                            dataEntry['NBL'] = "1";
                        }
                    } else if (key === 'Conformité physico-chimique') {
                        if (value === 'oui') {
                            dataEntry['CCL'] = "1";
                            dataEntry['NCL'] = "0";
                        } else if (value === 'non') {
                            dataEntry['CCL'] = "0";
                            dataEntry['NCL'] = "1";
                        }
                    } else if (key === 'Respect des références de qualité') {
                        if (value === 'oui') {
                            dataEntry['CBR'] = "1";
                            dataEntry['NBR'] = "0";
                            dataEntry['CCR'] = "1";
                            dataEntry['NCR'] = "0";
                        } else if (value === 'non') {
                            dataEntry['CBR'] = "0";
                            dataEntry['NBR'] = "1";
                            dataEntry['CCR'] = "0";
                            dataEntry['NCR'] = "1";
                        }
                    }
                });
            }

            allData.push(dataEntry);
            processedFiles++;

            // Si tous les fichiers ont été traités, générer l'Excel
            if (processedFiles === htmlFiles.length) {
                // Générer un fichier Excel avec les données
                const outputFileName = `CNF_${new Date().toISOString()}.xlsx`;
                const outputFilePath = path.join('./excel', outputFileName);

                if (!fs.existsSync('./excel')) {
                    fs.mkdirSync('./excel');
                }

                // Convertir les données en format approprié pour XLSX
                const worksheet = XLSX.utils.json_to_sheet(allData);
                const workbook = XLSX.utils.book_new();
                XLSX.utils.book_append_sheet(workbook, worksheet, "data");

                // Écrire le fichier Excel
                XLSX.writeFile(workbook, outputFilePath);
                console.log(`Fichier Excel exporté sous ${outputFilePath}`);
            }
        }
    } catch (err) {
        console.error('Erreur:', err);
    }
}

// Appeler la fonction pour traiter les fichiers
processFiles();
