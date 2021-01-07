package com.github.kernelpanicaus.ddb_loader;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.lambda.runtime.events.models.s3.S3EventNotification;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;


/**
 * The Class LambdaFunctionHandler.
 * This application loads GZIPped Dynamo JSON file to DynamoDB.
 */
public class LambdaFunctionHandler implements RequestHandler<S3Event, Report> {

    /**
     * Provide the AWS region which your DynamoDB table is hosted.
     */
    static final String AWS_REGION = System.getenv("AWS_REGION");

    /**
     * The DynamoDB table name.
     */
    static final String DYNAMO_TABLE_NAME = System.getenv("DYNAMO_TABLE_NAME");

    /**
     * Configurable batch size
     */
    static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE","25"));

    static final ClientConfiguration config = new ClientConfiguration()
            .withMaxConnections(ClientConfiguration.DEFAULT_MAX_CONNECTIONS * 2);

    final AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
            .withClientConfiguration(config)
            .withRegion(AWS_REGION)
            .build();

    final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder
            .standard()
            .withClientConfiguration(config)
            .withRegion(AWS_REGION)
            .build();

    final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

    public Report handleRequest(S3Event s3event, Context context) {
        long startTime = System.currentTimeMillis();
        Report statusReport = new Report();
        LambdaLogger logger = context.getLogger();

        logger.log("Lambda Function Started");

        try {
            S3EventNotification.S3EventNotificationRecord record = s3event.getRecords().get(0);
            String srcBucket = record.getS3().getBucket().getName();
            String srcKey = record.getS3().getObject().getUrlDecodedKey();

            S3Object s3Object = s3Client.getObject(new GetObjectRequest(srcBucket, srcKey));
            statusReport.setFileSize(s3Object.getObjectMetadata().getContentLength());

            logger.log("S3 Event Received: " + srcBucket + "/" + srcKey);

            GZIPInputStream gis = new GZIPInputStream(s3Object.getObjectContent());
            Scanner fileIn = new Scanner(gis);

            TableWriteItems energyDataTableWriteItems = new TableWriteItems(DYNAMO_TABLE_NAME);

            List<Item> itemList = new ArrayList<Item>();

            while (fileIn.hasNext()) {
                Item item = Item.fromJSON(fileIn.nextLine());
                itemList.add(item);
            }

            for (List<Item> partition : Lists.partition(itemList, BATCH_SIZE)) {
                energyDataTableWriteItems.withItemsToPut(partition);
                BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(energyDataTableWriteItems);

                do {

                    Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();

                    if (outcome.getUnprocessedItems().size() > 0) {
                        logger.log("Retrieving the unprocessed " + outcome.getUnprocessedItems().size() + " items.");
                        outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                    }

                } while (outcome.getUnprocessedItems().size() > 0);
            }

            logger.log("Load finish in " + (System.currentTimeMillis() - startTime) + "ms");
            fileIn.close();
            gis.close();
            s3Object.close();

            statusReport.setStatus(true);
        } catch (Exception ex) {
            logger.log(ex.getMessage());
        }

        statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
        return statusReport;
    }
}
