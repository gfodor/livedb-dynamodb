AWS = require "aws-sdk"
_ = require "lodash"
async = require "async"
liveDbDynamoDB = require "../lib/dynamodb"

AWS.config.update accessKeyId: "TEST", secretAccessKey: "TEST", region: "local"

# Run this using DynamoDB local and fakes3
# http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html
# java -jar DynamoDBLocal.jar
#
# gem install fakes3
# fakes3 -r ~/fakes3 -p 8001

clear = (cb) ->
  dynamodb = new AWS.DynamoDB(endpoint: "http://localhost:8000", sslEnabled: false)
  s3 = new AWS.S3(endpoint: "http://localhost:8001", s3ForcePathStyle: true, sslEnabled: false)

  s3.createBucket
    Bucket: "livedbTest"
    (err) ->
      liveDbDynamoDB(dynamodb, s3, { bucketName: "livedbTest" }).purgeDocTable "testcollection", 1, 1, (err) ->
        liveDbDynamoDB(dynamodb, s3, { bucketName: "livedbTest" }).purgeOpsTable "testcollection_ops", 1, 1, (err) ->
          cb(err, dynamodb, s3)

create = (callback) ->
  clear (err, dynamodb, s3) ->
    callback liveDbDynamoDB(dynamodb, s3, { bucketName: "livedbTest" })

describe 'dynamodb', ->
  afterEach (done) ->
    clear done

  require('livedb/test/snapshotdb') create
  require('livedb/test/oplog') create
