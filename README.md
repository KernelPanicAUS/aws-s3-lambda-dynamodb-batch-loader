# aws-s3-lambda-dynamodb-bulk-loader
Forked from https://github.com/pauldeng/aws-s3-lambda-dynamodb-csv-loader/ 

## Overview
This is a bulk loader that reads compressed JSON from S3 and uses the DynamoDB batch write API to load the items into the configure table.

## Requirements

* Java 11
* Maven