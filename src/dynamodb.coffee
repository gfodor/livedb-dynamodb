zlib = require 'zlib'
async = require 'async'
retry = require 'retry'
_ = require 'lodash'

exports = module.exports = (dynamodb, s3, options) ->
  new LiveDbDynamoDB(dynamodb, s3, options)

r = (callback, routine) ->
  op = retry.operation
    retries: 5

  op.attempt (num) ->
    routine ->
      args = _.toArray(arguments)

      return if op.retry(args[0])
      callback.apply(this, args)

class LiveDbDynamoDB
  constructor: (@dynamodb, @s3, options) ->
    throw new Error("Must specify S3 bucket.") unless options.bucketName
    @bucketName = options.bucketName

  close: (callback) ->

  getSnapshot: (cName, docName, callback) ->
    r callback, (retry) =>
      @dynamodb.getItem
        TableName: cName
        Key: { id: { S: docName } }
        ConsistentRead: true
        (err, data) =>
          unless data
            retry(err, null)
          else
            if data.Item?
              r callback, (retry) =>
                @s3.getObject
                  Bucket: @bucketName
                  Key: "#{cName}/#{docName}"
                  (err, object) ->
                    unless object
                      retry(err, null)
                    else
                      castDocToSnapshot data.Item, object.Body, callback
            else
              callback(null, null)

  bulkGetSnapshot: (requests, callback) ->
    requestItems = {}
    results = {}
    docData = {}

    s3Requests = []

    for cName, docNames of requests
      requestItems[cName] =
        Keys: _.map(docNames, (n) -> { id: { S: n } })
        ConsistentRead: true

      results[cName] = {}
      docData[cName] = {}

      for docName in docNames
        s3Requests.push ((docName, cName) =>
          (cb) =>
            r cb, (retry) =>
              @s3.getObject
                Bucket: @bucketName
                Key: "#{cName}/#{docName}"
                (err, data) ->
                  return retry(err) if err && err.code != 'NotFound'

                  if data?.Body?
                    docData[cName][docName] = data.Body

                  cb(null))(docName, cName)

    async.parallelLimit s3Requests, 16, (err) =>
      return callback(err) if err

      r callback, (retry) =>
        @dynamodb.batchGetItem
          RequestItems: requestItems
          (err, data) ->
            return retry(err) if err

            async.each _.keys(data.Responses),
              ((cName, nextMap) ->
                async.map data.Responses[cName],
                  ((item, cb) ->
                    docName = docVal(item, "id")
                    castDocToSnapshot(item, docData[cName][docName], cb)),

                  (err, snapshots) ->
                    for snapshot in snapshots
                      results[cName][snapshot.docName] = snapshot

                    nextMap(err)),
              ((err) -> callback(err, results))

  writeSnapshot: (cName, docName, data, callback) ->
    castSnapshotToDoc docName, data, (err, doc) =>
      return callback(err) if err

      async.parallel([
        ((cb) =>
          r cb, (retry) =>
            @dynamodb.putItem
              TableName: cName
              Item: doc.item
              (err, data) ->
                return retry(err) if err
                cb(err, data)),
        ((cb) =>
          r cb, (retry) =>
            @s3.putObject
              Bucket: @bucketName
              Key: "#{cName}/#{docName}"
              Body: doc.object
              (err, data) ->
                return retry(err) if err
                cb(err, data)),
        ], callback)

  getOplogCollectionName: (cName) -> "#{cName}_ops"

  writeOp: (cName, docName, opData, callback) ->
    castOpToDoc docName, opData, (err, doc) =>
      async.parallel([
        ((cb) =>
          r cb, (retry) =>
            @dynamodb.putItem
              TableName: this.getOplogCollectionName(cName)
              Item: doc.item
              Expected:
                op_id:
                  Exists: false
              (err, data) ->
                if !err || (err.code? && err.code == 'ConditionalCheckFailedException')
                  cb(null)
                else
                  retry(err)),
        ((cb) =>
          if doc.object
            r cb, (retry) =>
              @s3.putObject
                Bucket: @bucketName
                Key: "#{this.getOplogCollectionName(cName)}/#{doc.item.op_id.S}"
                Body: doc.object
                (err, data) ->
                  return retry(err) if err
                  cb(err, data)
          else
            cb(null))], callback)


  getVersion: (cName, docName, callback) =>
    @dynamodb.query
      TableName: this.getOplogCollectionName(cName)
      Select: 'SPECIFIC_ATTRIBUTES'
      AttributesToGet: ['v']
      Limit: 1
      ConsistentRead: true
      KeyConditions:
        doc_id:
          AttributeValueList: [{ S: docName }]
          ComparisonOperator: 'EQ'
      ScanIndexForward: false
      (err, data) ->
        return callback(err, 0) if err || !data || data.Items.length == 0
        return callback(err, docVal(_.first(data.Items), "v") + 1)

  getOps: (cName, docName, start, end, callback) ->
    [start, end] = [end, start] if end && start && end < start

    keyConditions =
      doc_id:
        AttributeValueList: [{ S: docName }]
        ComparisonOperator: 'EQ'

    if end?
      if end == start
        # This is weird, but tests pass
        keyConditions.v =
          AttributeValueList: [{ N: (start + 1).toString() }]
          ComparisonOperator: 'EQ'
      else
        keyConditions.v =
          AttributeValueList: [{ N: start.toString() }, { N: (end - 1).toString() }]
          ComparisonOperator: 'BETWEEN'
    else
      keyConditions.v =
        AttributeValueList: [{ N: start.toString() }]
        ComparisonOperator: 'GE'

    @dynamodb.query
      TableName: this.getOplogCollectionName(cName)
      Select: 'ALL_ATTRIBUTES'
      ConsistentRead: true
      KeyConditions: keyConditions
      ScanIndexForward: true
      (err, data) =>
        return callback(err, []) if err || !data

        async.mapLimit data.Items, 16, ((item, cb) =>
          if item.data
            castItemToOp(item, cb)
          else
            @s3.getObject
              Bucket: @bucketName
              Key: "#{this.getOplogCollectionName(cName)}/#{item.op_id.S}"
              (err, object) ->
                return cb(err, null) if err
                decodeValue object.Body, cb), callback

  purgeDocTable: (name, readCapacity, writeCapacity, cb) ->
    purgeTable @dynamodb, {
      TableName: name
      AttributeDefinitions: [
        { AttributeName: "id", AttributeType: "S" },
      ],
      KeySchema: [
        { AttributeName: "id", KeyType: "HASH" },
      ],
      ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
    }, cb

  purgeOpsTable: (name, readCapacity, writeCapacity, cb) ->
    purgeTable @dynamodb, {
      TableName: name,
      AttributeDefinitions: [
        { AttributeName: "doc_id", AttributeType: "S" },
        { AttributeName: "v", AttributeType: "N" },
      ],
      KeySchema: [
        { AttributeName: "doc_id", KeyType: "HASH" }
        { AttributeName: "v", KeyType: "RANGE" }
      ],
      ProvisionedThroughput: { ReadCapacityUnits: readCapacity, WriteCapacityUnits: writeCapacity },
    }, cb

purgeTable = (db, tableDefinition, cb) ->
  tableName = tableDefinition.TableName

  initTable = (err, initDone) ->
    db.createTable tableDefinition, (err, data) ->
      return cb(err) if err

      tablePending = true

      async.whilst ( -> tablePending ),
        ((done) ->
          db.describeTable TableName: tableName, (err, tableInfo) ->
            tablePending = !tableInfo? || tableInfo.Table.TableStatus != 'ACTIVE'

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

castOpToDoc = (docName, opData, cb) ->
  opData = _.clone(opData)
  opV = opData.v

  encodeValue opData, (err, encodedData) ->
    return cb(err) if err

    encodedBase64Data = encodedData.toString('base64')

    doc =
      item:
        doc_id: { S: docName }
        op_id: { S: "#{docName}_#{opV}" }
        v: { N: opV.toString() }

    # If large operation, store in S3. Otherwise use DynamoDB.
    if encodedBase64Data.length > 1024 * 32
      doc.object = encodedData
    else
      doc.item.data = { B: encodedBase64Data }

    cb err, doc

castItemToOp = (doc, cb) ->
  return cb(null) unless doc
  decodeValue docVal(doc, "data"), cb

castSnapshotToDoc = (docName, data, cb) ->
  encodeValue data.data, (err, encodedData) ->
    encodeValue data.m, (err, encodedM) ->
      return cb(err) if err

      doc =
        item:
          id: { S: docName }
          type: { S: (data.type || "").toString() }
          v: { N: data.v.toString() }
          m: { B: encodedM.toString('base64') }
        object: encodedData

      cb(err, doc)

castDocToSnapshot = (doc, object, cb) ->
  return cb(null) unless doc

  decodeValue object, (err, data) ->
    decodeValue docVal(doc, "m"), (err, m) ->
      return cb(err) if err

      snapshot =
        docName: docVal(doc, "id")
        v: null
        type: null
        data: null
        m: null

      type = docVal(doc, "type")
      type = null if type == "null"
      v = docVal(doc, "v")

      snapshot.type = type if type? && type != ""
      snapshot.v = v if v? && v != ""
      snapshot.data = data if data?
      snapshot.m = m if m?

      cb err, snapshot

decodeValue = (v, cb) ->
  zlib.inflate v, (err, inflated) ->
    if err
      cb(err, null)
    else
      value = if inflated.length > 0 then JSON.parse(inflated) else null
      cb(null, value)

encodeValue = (v, cb) ->
  return cb(null, null) if v == null

  zlib.deflate JSON.stringify(v), (err, deflated) ->
    if err
      cb(err, null)
    else
      cb(null, deflated)

docVal = (doc, attr) ->
  attribute = doc[attr]

  type = _.first(_.keys(attribute))
  value = attribute[type]

  if type == 'N'
    if value == "" then null else _.parseInt(value)
  else if type == 'B'
    new Buffer(value, 'base64')
  else
    value
