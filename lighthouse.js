require('dotenv').config();
const psi = require('psi');
const csv = require("csvtojson");
const { BlobServiceClient, StorageSharedKeyCredential } = require("@azure/storage-blob");
const fs = require('fs');




(async () => {

    const azAccountName = process.env.AZ_ACCOUNT;
    const azAccountKey = process.env.AZ_KEY;
    const azContainer = process.env.AZ_CONTAINER_NAME;
    const azBlobName = process.env.AZ_BLOB_NAME;

    const insightAPIKey = process.env.GOOGLE_API;

    const foldername_output = 'output';
    const filename_download = 'data.csv';
    const filename_errors = `failedpages_${azBlobName}.csv`;

    
    // DOWNLOAD DATASET TO PATH DEFINED IN .ENV.
    const sharedKeyCredential = new StorageSharedKeyCredential(azAccountName, azAccountKey);

    const blobServiceClient = new BlobServiceClient(`https://${azAccountName}.blob.core.windows.net`, sharedKeyCredential);

    const containerClient = blobServiceClient.getContainerClient(azContainer);

    const blobClient = containerClient.getBlobClient(azBlobName);
    
    await blobClient.downloadToFile(filename_download, 0, undefined, undefined);
    
    console.log('downloaded file from azure..')


    // OPEN DATASET.
    csv().fromFile(filename_download).then( async (content) => {

        // SETUP RESULTS OUTPUT.
        if (content.length > 0 && !fs.existsSync(foldername_output)) {

            const csvColumns = Object.keys(content[0]).toString() + ',second_err';
            console.log(csvColumns)
            fs.mkdirSync(foldername_output);

            fs.writeFile(`${foldername_output}/${filename_errors}`, csvColumns, err => {
                if (err) console.error(err);
            });
            console.log('created internal folders etc..')
        };


        // START ANALYSIS.
        let crashCounter = 0;
        let progress = 0;
        const totalAmount = content.length;
        const batchSize = 10; // max amount per API KEY.
        const batches = [];
        
        for (let i = 0; i < totalAmount; i += batchSize) {
            const batch = content.slice(i, i + batchSize);
            batches.push(batch);
        };


        for (const batch of batches) {

            await Promise.allSettled(batch.map( async company => {

                const newBlob = containerClient.getBlockBlobClient(`results/${company.orgnr}.json`);
                
                const doesExist = await newBlob.exists();

                if (!doesExist) {

                    await psi(company.web, {
                        nokey: insightAPIKey,
                        strategy: 'desktop',
                        format: 'json'
                    }).then( async (data) => {

                        crashCounter = 0;

                        console.log('successfully analysed webpage..');

                        await newBlob.upload(JSON.stringify(data), JSON.stringify(data).length);

                        /*
                        fs.writeFile(`${foldername_output}/${company.orgnr}.json`, JSON.stringify(data), err => {
                            if (err) console.error(err);
                        });
                        */

                    }).catch( async (err) => {

                        const failedOutput = '\n' + Object.values(company).toString() + ';' + err.code;

                        fs.appendFile(`${foldername_output}/${filename_errors}`, failedOutput, 'utf-8', err => {
                            if (err) console.error(err);
                        });

                        console.log('failed to analyse webpage.. due to:', err.code);

                        if (err.code === 429) {
                            crashCounter += 1;
                        };

                    })
            };
            }));

            if (crashCounter > 8) {

                await new Promise(resolve => setTimeout(resolve, 300000)); // PAUSE PROGRAM

            };

        
            
            // LOG PROGRESS IN CONSOLE.
            progress += batchSize;
            console.log('\n', 'PROGRESS:', progress,  'of', totalAmount, '\n')

        };

    }).then(async () => {

        const newBlob = containerClient.getBlockBlobClient(filename_errors);

        await newBlob.uploadFile(`${foldername_output}/${filename_errors}`);
        
    });

})();