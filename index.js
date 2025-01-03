const puppeteer = require('puppeteer');
const fs = require('fs').promises;
const xlsx = require('xlsx');
const path = require('path');

// Fonction pour lire les données depuis le fichier Excel
const readExcelData = (filePath) => {
    const workbook = xlsx.readFile(filePath);
    const sheetName = workbook.SheetNames[0];
    const sheet = workbook.Sheets[sheetName];
    return xlsx.utils.sheet_to_json(sheet);
};

// Fonction pour obtenir le nom de répertoire basé sur la date
const getTodayDirectory = () => {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');
    const hours = String(today.getHours()).padStart(2, '0');
    const minutes = String(today.getMinutes()).padStart(2, '0');
    return path.join(__dirname, 'html', `${year}-${month}-${day}`);
};

// Fonction principale pour traiter une URL
const processUrl = async (urlData, todayDir) => {
    const browser = await puppeteer.launch({ headless: true });
    const page = await browser.newPage();

    try {
        // Visitez l'URL
        await page.goto('https://orobnat.sante.gouv.fr/orobnat/', { waitUntil: 'networkidle2' });
        await page.goto(urlData.url, { waitUntil: 'networkidle2' });

        // Récupérez le contenu HTML
        const htmlContent = await page.content();

        // Enregistrez le contenu HTML
        const filePath = path.join(todayDir, `${urlData.code}_${urlData.reseau}.html`);
        await fs.writeFile(filePath, htmlContent);
        console.log(`${urlData.code} enregistré.`);
    } catch (error) {
        console.error(`Erreur pour l'URL ${urlData.url}:`, error.message);
    } finally {
        await browser.close();
    }
};

// Fonction pour gérer les workers en parallèle
const runWorkers = async (urls, workerCount = 5) => {
    const todayDir = getTodayDirectory();
    await fs.mkdir(todayDir, { recursive: true });

    // Divisez les URLs en lots pour chaque worker
    const urlChunks = Array.from({ length: workerCount }, (_, i) =>
        urls.filter((_, index) => index % workerCount === i)
    );

    // Lancer les workers
    const promises = urlChunks.map(chunk =>
        (async () => {
            for (const urlData of chunk) {
                await processUrl(urlData, todayDir);
            }
        })()
    );

    // Attendez que tous les workers aient terminé
    await Promise.all(promises);
};

(async () => {
    try {
        const urls = readExcelData('./urls.xlsx');
        const workerCount = 5; // Nombre de workers (ajustez selon vos ressources)
        await runWorkers(urls, workerCount);
        console.log("Traitement terminé.");
    } catch (error) {
        console.error("Erreur principale :", error.message);
    }
})();
