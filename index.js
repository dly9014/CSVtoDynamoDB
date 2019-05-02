var fs = require('fs');
var parse = require('csv-parse');
var async = require('async');
const AWS = require('aws-sdk');
const dynamodbDocClient = new AWS.DynamoDB({ region: "us-east-1" });

var csv_filename = "./VCS-tags.csv";

rs = fs.createReadStream(csv_filename);
parser = parse({
    columns : false,
    delimiter : ','
}, function(err, data) {
    // console.log('data =')
    // console.log(JSON.stringify(data))
    var split_arrays = [], size = 25;

    while (data.length > 0) {

        //split_arrays.push(data.splice(0, size));
        let cur25 = data.splice(0, size)
        // console.log('cur25 =' + cur25)
        // console.log('cur25.length = ' + cur25.length)
        let item_data = []

        for (var i = cur25.length - 1; i >= 0; i--) {
            // console.log('cur25[i] = ' + cur25[i])
            // console.log('type of cur25[i] = ' + typeof(cur25[i]))
            let cur25str = JSON.stringify(cur25[i])
            cur25str = cur25str.replace(/\[|{|\]|}|"/g, "")
            cur25str = cur25str.replace(/:|,/g, "\n");
            console.log('cur25str = ' + cur25str)
          const this_item = {
            "PutRequest" : {
              "Item": {
                // your column names here will vary, but you'll need do define the type
                "tag": {
                  "S": cur25str
                }
              }
            }
          };
          item_data.push(this_item)
        }
        split_arrays.push(item_data);
    }
    data_imported = false;
    chunk_no = 1;
    async.each(split_arrays, (item_data, callback) => {
        const params = {
            RequestItems: {
                "VirtualConciergeTag" : item_data
            }
        }
        dynamodbDocClient.batchWriteItem(params, function(err, res, cap) {
            if (err === null) {
                console.log('Success chunk #' + chunk_no);
                data_imported = true;
            } else {
                console.log(err);
                console.log('Fail chunk #' + chunk_no);
                data_imported = false;
            }
            chunk_no++;
            callback();
        });

    }, () => {
        // run after loops
        console.log('all data imported....');

    });

});
rs.pipe(parser);