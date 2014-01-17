#!/usr/bin/env node

liveDbDynamoDB = require('../lib/dynamodb');
util = require("util");

argv = require('optimist')
  .usage('Create DynamoDB tables for livedb.\nWARNING: Any existing tables will be overwritten.\n\nUsage: livedb-dynamodb-init -t NAME')
  .alias('t', 'table-name')
  .describe('t', 'Name of document and ops tables to create.\n    (Ex. "foo" will create tables "foo" and "foo_ops")')
  .alias('c', 'aws-credentials')
  .demand('c')
  .describe('c', 'Path to AWS credentials JSON\n    (in form: { "accessKeyId": ACCESS_KEY, "secretAccessKey": SECRET_KEY, "region": REGION })')
  .demand('t')
  .alias('l', 'local')
  .describe('l', 'Run on local DynamoDB')
  .describe('r', 'DynamoDB read capacity units to initialize tables with.')
  .describe('w', 'DynamoDB write capacity units to initialize tables with.')
  .alias('r', 'read-capacity')
  .alias('w', 'write-capacity')
  .default('r', 1)
  .default('w', 1)
  .argv

AWS = require("aws-sdk")

if (argv.c) {
  AWS.config.loadFromPath(argv.c);
}

var dynamodb = null;

if (argv.l) {
  dynamodb = new AWS.DynamoDB({ endpoint: "http://localhost:8000", sslEnabled: false });
} else {
  dynamodb = new AWS.DynamoDB();
}

s3 = new AWS.S3();

util.log("Creating table " + argv.t + ".");

liveDbDynamoDB(dynamodb, s3, { bucketName: "noBucket" }).purgeDocTable(argv.t, argv.r, argv.w, function(err) {
  if (err) { return util.log(err); }

  util.log("Creating table " + argv.t + "_ops" + ".");

  liveDbDynamoDB(dynamodb, s3, { bucketName: "noBucket" }).purgeOpsTable(argv.t + "_ops", argv.r, argv.w, function(err) {
    if (err) { return util.log(err); }

    util.log("Finished.")
  });
});
