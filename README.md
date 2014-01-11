livedb-dynamodb
===============

DynamoDB adapter for LiveDB.

As with `livedb-mongo`, snapshots are stored in the main table and operations
are stored in `TABLE_ops`. 

# Usage

`livedb-dynamodb` wraps a `AWS.DynamoDB` object from the `aws-sdk` npm package. 

```
AWS = require("aws-sdk");

var livedbdynamodb = require('livedb-dynamodb');
var dynamodb = livedbdynamodb(new AWS.DynamoDB());

var livedb = require('livedb').client(dynamodb);
```

# Creating Tables

The package includes a small commandline utility to create your tables. First,
you'll need a `aws.json` file with your credentials of the form:

```
{ "accessKeyId": "ACCESS_KEY", "secretAccessKey": "SECRET_ACCESS_KEY",
"region": "REGION" }
```

Where `region` is `us-west-1`, etc.

To create tables, run:

```
node_modules/livedb-dynamodb/bin/init.js
```

You'll need to specify `-t` for the table name and your credentials file with
`-c`. You can also specify `--local` if you have a running instance of [Local
DynamoDB] [1]. For example:

```
node_modules/livedb-dynamodb/bin/init.js -t -c credentials.json --local
```

[1]:
http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

