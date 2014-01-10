AWS = require "aws-sdk"
_ = require "lodash"
async = require "async"
liveDbDynamoDB = require "../lib/dynamodb"

AWS.config.update accessKeyId: "TEST", secretAccessKey: "TEST", region: "local"

# Run this using DynamoDB local
# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

purgeTable = (tableDefinition, cb) ->
  db = new AWS.DynamoDB(endpoint: "http://localhost:8000", sslEnabled: false)
  tableName = tableDefinition.TableName

  initTable = (err, initDone) ->
    db.createTable tableDefinition, (err, data) ->
      tablePending = true

      async.whilst ( -> tablePending ),
        ((done) ->
          db.describeTable TableName: tableName, (err, tableInfo) ->
            tablePending = !tableInfo? || tableInfo.Table.TableStatus != 'ACTIVE'
            console.log(tableInfo)

            if tablePending
              setTimeout(done, 1000)
            else
              done()),
        initDone

  db.listTables (err, data) ->
    if _.contains(data?.TableNames, tableName)
      db.deleteTable TableName: tableName, (err, data) ->
        tableExists = true

        async.whilst (-> tableExists),
          ((done) ->
            db.describeTable TableName: tableName, (err, tableInfo) ->
              tableExists = tableInfo?

              if tableExists
                setTimeout(done, 1000)
              else
                done()),
          ((err) -> initTable(err, cb))
    else
      initTable(null, cb)

purgeDocTable = (name, cb) ->
  purgeTable {
    TableName: name
    AttributeDefinitions: [
      { AttributeName: "id", AttributeType: "S" },
      { AttributeName: "v", AttributeType: "N" },
    ],
    KeySchema: [
      { AttributeName: "id", KeyType: "HASH" },
      { AttributeName: "v", KeyType: "RANGE" },
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }, cb

purgeOpsTable = (name, cb) ->
  purgeTable {
    TableName: name,
    AttributeDefinitions: [
      { AttributeName: "doc_id", AttributeType: "S" },
      { AttributeName: "v", AttributeType: "N" },
    ],
    KeySchema: [
      { AttributeName: "doc_id", KeyType: "HASH" }
      { AttributeName: "v", KeyType: "RANGE" }
    ],
    ProvisionedThroughput: { ReadCapacityUnits: 1, WriteCapacityUnits: 1 },
  }, cb

clear = (cb) ->
  purgeDocTable "docs", (err) ->
    purgeOpsTable "docs_ops", cb

create = (callback) ->
  clear ->
    dynamodb = new AWS.DynamoDB(endpoint: "http://localhost:8000", sslEnabled: false)
    callback liveDbDynamoDB dynamodb

describe 'dynamodb', ->
  afterEach (done) ->
    clear done

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
