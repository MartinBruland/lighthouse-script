require("dotenv").config();
const csv = require("csvtojson");
const fs = require("fs");
const fetch = require("node-fetch");
const {
  BlobServiceClient,
  StorageSharedKeyCredential,
} = require("@azure/storage-blob");

function createAzureBlobClient(
  accountName,
  accountKey,
  containerName,
  blobName
) {

  const sharedKeyCredential = new StorageSharedKeyCredential(
    accountName,
    accountKey
  );

  const blobServiceClient = new BlobServiceClient(
    `https://${accountName}.blob.core.windows.net`,
    sharedKeyCredential
  );

  const containerClient = blobServiceClient.getContainerClient(containerName);

  const blobClient = containerClient.getBlockBlobClient(blobName);

  //const blobClient = containerClient.getBlobClient(blobName);

  return blobClient;
}

async function runLighthouseAnalysis(websiteURL, apiKey) {
  return new Promise((resolve, reject) => {

    const apiURL = "https://www.googleapis.com/pagespeedonline/v5/runPagespeed";

    const urlToAnalyse = "http://" + websiteURL;

    const categories = [
      "performance",
      "accessibility",
      "best_practices",
      "pwa",
      "seo",
    ];

    let url = apiURL + "?url=" + urlToAnalyse;

    categories.forEach((item) => {
      url += `&category=${item}`;
    });

    url += `&key=${apiKey}`;

    fetch(url).then(async (res) => {
      if (res.status >= 200 && res.status <= 299) {
        console.log("Analysis Successful.");
        resolve(await res.json());
      } else {
        console.log("Analysis Failed:", res.status);
        reject(res.status);
      }
    });
  });
}

function extractFromLighthouseReport(report, format) {

  const websiteURL = report.lighthouseResult.requestedUrl;
  const categories = report.lighthouseResult.categories; // Inneholder performance, accessibility, seo, best_practices og pwa..
  const audits = report.lighthouseResult.audits; // Inneholder begrunnelser..

  switch (format) {
    case 'json':
        
        let output = {
            url: websiteURL,
            results: [],
          };
        
          Object.entries(categories).map((item) => {
        
            const value = item[1];
        
            const categoryResults = {
              categoryTitle: value.title,
              categoryScore: value.score,
              categoryMetrics: [],
            };
        
            value.auditRefs.forEach((audit) => {
              const auditID = audit.id;
              const auditWeight = audit.weight;
        
              Object.entries(audits).map((audit) => {
                // Iterate all metrics + results..
        
                const lookupAuditID = audit[1].id;
                const lookupAuditTitle = audit[1].title;
                const lookupAuditDescription = audit[1].description;
                const lookupAuditDisplayValue = audit[1].displayValue;
                const lookupAuditScore = audit[1].score;
        
                if (auditID === lookupAuditID) {
                  categoryResults.categoryMetrics.push({
                    metricTitle: lookupAuditTitle,
                    metricDescription: lookupAuditDescription,
                    metricValue: lookupAuditDisplayValue,
                    metricWeight: auditWeight,
                    metricScore: lookupAuditScore,
                  });
                }
              });
            });
        
            output.results.push(categoryResults);
          });
        
          return output;

    case 'csv':

        const csvOutput = {
            columns: "url,performance_score,accessibility_score,seo_score,best_practices_score,pwa_score",
            row: websiteURL
        };

        Object.entries(categories).map((item) => {
        
            csvOutput.row += ',';
            csvOutput.row += item[1].score;
            
        });

        return csvOutput
  
    default:
        break;
  }

}

function writeToLocalFile(jsonString, localFilePath) {
  fs.writeFile(localFilePath, jsonString, (err) => {
    if (err) console.error(err);
  });
}

function appendToLocalFile(insert, localFilePath) {
  fs.appendFile(localFilePath, insert, "utf-8", (err) => {
    if (err) console.error(err);
  });
}

function addDirectory(title) {
  if (!fs.existsSync(title)) {
    fs.mkdirSync(title);
  }
}

(async () => {

  // GET DATA FROM ENVIRONMENT VARIABLES.
  const azAccountName = process.env.AZ_ACCOUNT;
  const azAccountKey = process.env.AZ_KEY;
  const azContainer = process.env.AZ_CONTAINER_NAME;
  const azBlobName = process.env.AZ_BLOB_NAME;
  const insightAPIKey = process.env.GOOGLE_API;

  // SET FOLDER NAMES.
  const folderDownloads = "dataset";
  const folderFailed = "failed";
  const folderOriginalResultsJSON = "report_original";
  const folderResultsJSON = "report_extract_json";
  const folderResultsCSV = "report_extract_csv";

  // CREATE FOLDERS.
  addDirectory(folderDownloads);
  addDirectory(folderFailed);
  addDirectory(folderOriginalResultsJSON);
  addDirectory(folderResultsJSON);
  addDirectory(folderResultsCSV);

  // SET FILE PATHS.
  const filepath_downloaded_data_csv = `${folderDownloads}/data.csv`;
  const filepath_errors_csv = `${folderFailed}/failed_${azBlobName}.csv`;
  const filepath_report_extract_csv = `${folderResultsCSV}/reportExtract.csv`;

  // DOWNLOAD DATASET FROM AZURE.
  const dataBlob = createAzureBlobClient(
    azAccountName,
    azAccountKey,
    azContainer,
    azBlobName,
    filepath_downloaded_data_csv
  );

  // DOWNLOAD DATASET.
  await dataBlob.downloadToFile(
    filepath_downloaded_data_csv,
    0,
    undefined,
    undefined
  );

  // OPEN DATASET.
  csv()
    .fromFile(filepath_downloaded_data_csv)
    .then(async (content) => {

      // KEEP TRACK OF PROGRESS.
      let progress = 0;
      const totalAmount = content.length;

      // SPLIT DATA INTO BATCHES.
      const batches = [];
      const batchSize = 10; // max amount per API KEY.
      for (let i = 0; i < totalAmount; i += batchSize) {
        const batch = content.slice(i, i + batchSize);
        batches.push(batch);
      };

      // RUN THROUGH BATCHES.
      for (const batch of batches) {
        progress += batchSize;
        console.log("\n", "STARTED:", progress, "OF", totalAmount, "\n");

        // SEND (batchSize) AMOUNT OF REQUESTS AT ONCE.
        await Promise.allSettled(
          batch.map(async (company) => {
            const companyID = company.orgnr;
            const companyURL = company.web;

            // RUN ANALYSIS
            await runLighthouseAnalysis(companyURL, insightAPIKey)
              .then(async (report) => {

                const originalDataset = JSON.stringify(report);
                const jsonExtract = extractFromLighthouseReport(report, 'json');
                const extractedDatasetJSON = JSON.stringify(jsonExtract);
                const csvExtract = extractFromLighthouseReport(report, 'csv');
                

                // UPLOAD ORIGINAL REPORT.
                const filepath_report_original = `${folderOriginalResultsJSON}/${companyID}.json`;
                const blobOriginalJSON = createAzureBlobClient(
                  azAccountName,
                  azAccountKey,
                  azContainer,
                  filepath_report_original
                );
                await blobOriginalJSON.upload(originalDataset, originalDataset.length);
                //writeToLocalFile(originalDataset, filepath_report_original);

                // UPLOAD JSON EXTRACT OF REPORT.
                const filepath_report_extract_json = `${folderResultsJSON}/${companyID}.json`;
                const blobExtractJSON = createAzureBlobClient(
                  azAccountName,
                  azAccountKey,
                  azContainer,
                  filepath_report_extract_json
                );
                await blobExtractJSON.upload(extractedDatasetJSON, extractedDatasetJSON.length);
                //writeToLocalFile(extractedDatasetJSON, filepath_report_extract_json);

                // UPLOAD CSV EXTRACT OF REPORT.
                if (!fs.existsSync(filepath_report_extract_csv)) {
                    const insertColumns = csvExtract.columns;
                    writeToLocalFile(insertColumns, filepath_report_extract_csv);
                };
                const insertRow = `\n${csvExtract.row}`;
                appendToLocalFile(insertRow, filepath_report_extract_csv);

              })
              .catch((err) => {

                // STORE FAILED PAGES IN CSV.
                if (!fs.existsSync(filepath_errors_csv)) {
                  const insertColumns = Object.keys(content[0]).toString() + ",error_msg";
                  writeToLocalFile(insertColumns, filepath_errors_csv);
                }

                const insertRow = "\n" + Object.values(company).toString() + "," + err;
                appendToLocalFile(insertRow, filepath_errors_csv);

              });
          })
        );
      }
    })
    .then(async () => {

        // UPLOAD CSV EXTRACT.
        const blobExtractCSV = createAzureBlobClient(
            azAccountName,
            azAccountKey,
            azContainer,
            filepath_report_extract_csv,
        );

        await blobExtractCSV.uploadFile(filepath_report_extract_csv);


        // UPLOAD DATASET OF FAILED PAGES FROM ./failedPages/
        const failedBlob = createAzureBlobClient(
            azAccountName,
            azAccountKey,
            azContainer,
            filepath_errors_csv
        );

        await failedBlob.uploadFile(filepath_errors_csv);

    });
})();
