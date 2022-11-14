const fs = require('fs');
const csvSplitStream = require('csv-split-stream');
 
return csvSplitStream.split(
  fs.createReadStream('data.csv'),
  {
    lineLimit: 8000
  },
  (index) => fs.createWriteStream(`output-${index}.csv`)
)
.then(csvSplitResponse => {
  console.log('csvSplitStream succeeded.', csvSplitResponse);
  // outputs: {
  //  "totalChunks": 350,
  //  "options": {
  //    "delimiter": "\n",
  //    "lineLimit": "10000"
  //  }
  // }
}).catch(csvSplitError => {
  console.log('csvSplitStream failed!', csvSplitError);
});
