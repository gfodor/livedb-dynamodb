DynamoDB adapter for LiveDB.

As with `livedb-mongo`, snapshots are stored in the main table and operations are stored in `TABLE_ops`. 

## Usage

`livedb-dynamodb` wraps a `AWS.DynamoDB` object from the `aws-sdk` npm package. 

```
AWS = require("aws-sdk");

var livedbdynamodb = require('livedb-dynamodb');
var dynamodb = livedbdynamodb(new AWS.DynamoDB());

var livedb = require('livedb').client(dynamodb);
```

## Creating Tables

The package includes a small commandline utility to create your tables. First, you'll need a `aws.json` file with your credentials of the form:

```
{ "accessKeyId": "ACCESS_KEY", "secretAccessKey": "SECRET_ACCESS_KEY", "region": "REGION" }
```

Where `region` is `us-west-1`, etc.

To create tables, run:

```
node_modules/livedb-dynamodb/bin/init.js
```

You'll need to specify `-t` for the table name and your credentials file with `-c`. You can also specify `--local` if you have a running instance of [Local DynamoDB] [1]. For example:

```
node_modules/livedb-dynamodb/bin/init.js -t -c credentials.json --local
```

[1]: http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Tools.DynamoDBLocal.html

## MIT License

Copyright (c) 2014 by Greg Fodor

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
