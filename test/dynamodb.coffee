AWS = require "aws-sdk"
_ = require "lodash"
async = require "async"
liveDbDynamoDB = require "../lib/dynamodb"

AWS.config.update accessKeyId: "TEST", secretAccessKey: "TEST", region: "local"

# Run this using DynamoDB local
# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

clear = (cb) ->
  dynamodb = new AWS.DynamoDB(endpoint: "http://localhost:8000", sslEnabled: false)
  
  liveDbDynamoDB(dynamodb).purgeDocTable "testcollection", 1, 1, (err) ->
    liveDbDynamoDB(dynamodb).purgeOpsTable "testcollection_ops", 1, 1, (err) ->
      cb(err, dynamodb)

create = (callback) ->
  clear (err, dynamodb) ->
    callback liveDbDynamoDB dynamodb

describe 'dynamodb', ->
  afterEach (done) ->
    clear done

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
