zlib = require 'zlib'
async = require 'async'
_ = require 'lodash'

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
          callback(err, null)

  bulkGetSnapshot: (requests, callback) ->
    requestItems = {}
    results = {}

    for cName, docNames of requests
      requestItems[cName] = { Keys: _.map(docNames, (n) -> { id: { S: n } }), ConsistentRead: true }
      results[cName] = {}

    # Warning: AWS has 1MB limit on this request. Might be an issue for larger docs?
    @dynamodb.batchGetItem
      RequestItems: requestItems
      (err, data) ->
        return callback(err) if err

        async.each _.keys(data.Responses),
          ((cName, nextMap) ->
            async.map data.Responses[cName], castDocToSnapshot, (err, snapshots) ->
              for snapshot in snapshots
                results[cName][snapshot.docName] = snapshot

              nextMap(err)),
          ((err) -> callback(err, results))

  writeSnapshot: (cName, docName, data, callback) ->
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
        TableName: this.getOplogCollectionName(cName)
        Item: doc
        Expected:
          op_id:
            Exists: false
        (err, data) ->
          if !err || (err.code? && err.code == 'ConditionalCheckFailedException')
            callback(null, data)
          else
            callback(err)

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
      (err, data) ->
        return callback(err, []) if err || !data
        async.map data.Items, castDocToOp, callback

castOpToDoc = (docName, opData, cb) ->
  opData = _.clone(opData)
  opV = opData.v

  encodeValue opData, (err, encodedData) ->
    return cb(err) if err

    cb err,
      doc_id: { S: docName }
      op_id: { S: "#{docName}_#{opV}" }
      v: { N: opV.toString() }
      data: { B: encodedData }

castDocToOp = (doc, cb) ->
  return cb(null) unless doc
  decodeValue docVal(doc, "data"), cb

castSnapshotToDoc = (docName, data, cb) ->
  encodeValue data.data, (err, encodedData) ->
    encodeValue data.m, (err, encodedM) ->
      return cb(err) if err

      cb err,
        id: { S: docName }
        type: { S: (data.type || "").toString() }
        v: { N: data.v.toString() }
        m: { B: encodedM }
        data: { B: encodedData }

castDocToSnapshot = (doc, cb) ->
  return cb(null) unless doc

  decodeValue docVal(doc, "data"), (err, data) ->
    decodeValue docVal(doc, "m"), (err, m) ->
      return cb(err) if err

      snapshot =
        docName: docVal(doc, "id")
        v: null
        type: null
        data: null
        m: null

      type = docVal(doc, "type")
      v = docVal(doc, "v")

      snapshot.type = type if type? && type != ""
      snapshot.v = v if v? && v != ""
      snapshot.data = data if data?
      snapshot.m = m if m?

      cb err, snapshot

decodeValue = (v, cb) ->
  zlib.inflate new Buffer(v, 'base64'), (err, inflated) ->
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
      cb(null, deflated.toString('base64'))

docVal = (doc, attr) ->
  attribute = doc[attr]

  type = _.first(_.keys(attribute))
  value = attribute[type]

  if type == 'N'
    if value == "" then null else _.parseInt(value)
  else
    value
