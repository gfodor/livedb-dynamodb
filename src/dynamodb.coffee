bson = require 'bson'
zlib = require 'zlib'

exports = module.exports = (dynamodb, snapTableName, opTableName, options) ->
  new LiveDbDynamoDB(dynamodb, options)

class LiveDbDynamoDB
  constructor: (@dynamodb, options) ->

  close: (callback) ->

  getSnapshot: (cName, docName, callback) ->
    @dynamodb.getItem
      TableName: cName
      Key: { id: { S: docName } }
      ConsistentRead: true
      (err, data) ->
        if data
          castDocToSnapshot data.Item, callback
        else
          callback(err, {})

  bulkGetSnapshot: (requests, callback) ->
    requestItems = {}

    for cName, docNames of requests
      requestItems[cName] = _.map(docNames, (n) -> { Keys: [{ id: { S: n }}], ConsistentRead: true })

    # Warning: AWS has 1MB limit on this request. Might be an issue for larger docs?
    @dynamodb.batchGetItem
      RequestItems: requestItems
      (err, data) ->
        return callback(err) if err

        results = {}

        async.each _.keys(data.Responses),
          ((cName, nextMap) ->
            async.map results[cName], castDocToSnapshot, (err, snapshots) ->
              results[cName] = snapshots
              nextMap(err)),
          ((err) -> callback(err, results))

  writeSnapshot: (cName, docName, data, callback) ->
    return callback("DynamoDB closed") if @closed

    castSnapshotToDoc docName, data, (err, doc) =>
      return callback(err) if err

      @dynamodb.putItem
        TableName: cName
        Item: doc
        callback

  getOplogCollectionName: (cName) -> "#{cName}_ops"

  writeOp: (cName, docName, opData, callback) ->
    castOpToDoc docName, opData, (err, doc) =>
      @dynamodb.putItem
        TableName: cName
        Item: doc
        callback

  getVersion: (cName, docName, callback) ->
    @dynamodb.query
      TableName: cName
      Select: 'SPECIFIC_ATTRIBUTES'
      AttributesToGet: ['v']
      Limit: 1
      ConsistentRead: true
      KeyConditions:
        id:
          AttributeValueList: [{ S: docName }]
          ComparisonOperator: 'EQ'
      ScanIndexForward: false
      (err, data) ->
        return callback(err, 0) if err || !data
        return callback(err, (docVal(_.first(data.Items), "v") || -1) + 1)

  getOps: (cName, docName, start, end, callback) ->
    keyConditions =
      id:
        AttributeValueList: [{ S: docName }]
        ComparisonOperator: 'EQ'

    if end
      keyConditions.v =
        AttributeValueList: [{ N: start }, { N: end - 1 }]
        ComparisonOperator: 'BETWEEN'
    else
      keyConditions.v =
        AttributeValueList: [{ N: start }]
        ComparisonOperator: 'GE'

    @dynamodb.query
      TableName: cName
      Select: 'ALL_ATTRIBUTES'
      ConsistentRead: true
      KeyConditions: keyConditions
      ScanIndexForward: true
      (err, data) ->
        return callback(err, []) if err || !data
        async.map data.Items, castDocToOp, callback

castOpToDoc = (docName, opData, cb) ->
  opData = _.clone(opData)
  opId = "#{docName} v#{opData.v}"
  opV = opData.v

  encodeValue opData.data, (err, encodedData) ->
    return cb(err) if err

    cb err,
      id: { S: opId }
      doc_id: { S: docName }
      v: { N: opV }
      data: { B: encodedData }

castDocToOp = (doc, cb) ->
  return unless doc
  decodeValue docVal(doc, "data"), cb

castSnapshotToDoc = (docName, data, cb) ->
  encodeValue data.data, (err, encodedData) ->
    encodeValue data.m, (err, encodedM) ->
      return cb(err) if err

      cb err,
        id: { S: docName }
        type: { N: (data.type || "").toString() }
        v: { N: data.v.toString() }
        m: { B: encodedM }
        data: { B: encodedData }

castDocToSnapshot = (doc, cb) ->
  return unless doc

  decodeValue docVal(doc, "data"), (err, data) ->
    decodeValue docVal(doc, "m"), (err, m) ->
      return cb(err) if err

      cb err,
        docName: docVal(doc, "id")
        data: data
        type: docVal(doc, "type")
        v: docVal(doc, "v")
        m: m

decodeValue = (v, cb) ->
  zlib.inflate new Buffer(v, 'base64'), (err, inflated) ->
    if err
      cb(err, null)
    else
      cb(null, bson.deserialize(inflated))

encodeValue = (v, cb) ->
  zlib.deflate bson.serialize(v, true, true), (err, deflated) ->
    if err
      cb(err, null)
    else
      cb(null, deflated.toString('base64'))

docVal(doc, attr) ->
  attribute = doc[attr]

  type = _.first(_.keys(attribute))
  value = attribute[type]

  return value unless type == 'N'
  if value == "" then null else _.parseInt(value)
