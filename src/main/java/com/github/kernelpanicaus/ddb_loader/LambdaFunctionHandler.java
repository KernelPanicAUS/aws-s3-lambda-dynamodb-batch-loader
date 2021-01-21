package com.github.kernelpanicaus.ddb_loader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.jayway.jsonpath.JsonPath;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutRequest;
import software.amazon.awssdk.services.dynamodb.model.WriteRequest;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.utils.IoUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * The Class LambdaFunctionHandler.
 * This application loads GZIPped Dynamo JSON file to DynamoDB.
 */
public class LambdaFunctionHandler implements RequestStreamHandler {

    /**
     * Provide the AWS region which your DynamoDB table is hosted.
     */
    static final Region AWS_REGION = Region.of(System.getenv("AWS_REGION"));

    /**
     * The DynamoDB table name.
     */
    // TODO: Make this dynamic, from the S3 event.
    static final String DYNAMO_TABLE_NAME = System.getenv("DYNAMO_TABLE_NAME");

    /**
     * Configurable batch size
     */
    static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "25"));
//
//    static final ClientConfiguration config = new ClientConfiguration()
//            .withMaxConnections(ClientConfiguration.DEFAULT_MAX_CONNECTIONS * 2);

    final S3Client s3Client = S3Client.builder()
            .region(AWS_REGION)
            .build();

    final DynamoDbClient dynamoDBClient = DynamoDbClient.builder()
            .region(AWS_REGION)
            .build();

    static AttributeValue toAttributeValue(Object value) {
        if (value == null) return AttributeValue.builder().nul(true).build();
        if (value instanceof AttributeValue) return (AttributeValue) value;
        if (value instanceof String) return AttributeValue.builder().s((String) value).build();
        if (value instanceof Number) return AttributeValue.builder().n(value.toString()).build();

        if (value instanceof Map) return AttributeValue.builder().m(
                ((Map<String, Object>) value).entrySet().stream().collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> toAttributeValue(e.getValue())
                ))).build();

        throw new UnsupportedOperationException("Time to impl new path for " + value);
    }

    public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) {
        // TODO: Add sanity check for dynamo table
        // TODO: Add dead letter queue for failures

        long startTime = System.currentTimeMillis();
        Report statusReport = new Report();
        LambdaLogger logger = context.getLogger();
        String event = null;
        try {
            event = IoUtils.toUtf8String(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }
        logger.log(event + "\n");
        logger.log("Lambda Function Started \n");

        try {

            String srcKey = JsonPath.read(event, "$.Records[0].s3.object.key");
            String srcBucket = JsonPath.read(event, "$.Records[0].s3.bucket.name");


            logger.log("Bucket name: " + srcBucket + "\n");
            logger.log("Key name: " + srcKey + "\n");
            logger.log("S3 Object: " + srcBucket + "/" + srcKey + "\n");

            logger.log("S3 Event Received: " + srcBucket + "/" + srcKey + "\n");

            ResponseInputStream<GetObjectResponse> responseInputStream = s3Client.getObject(
                    GetObjectRequest.builder()
                            .bucket(srcBucket)
                            .key(srcKey)
                            .build()
                    , ResponseTransformer.toInputStream()
            );

            logger.log("Reading input stream \n");

            GZIPInputStream gis = new GZIPInputStream(responseInputStream);
            Scanner fileIn = new Scanner(gis);
            var parser = new JSONParser();

            Collection<WriteRequest> itemList = new ArrayList<>();

            int counter = 0;
            int batchCounter = 0;
            while (fileIn.hasNext()) {
                var line = fileIn.nextLine();
//                logger.log(line + "\n");
                JSONObject jsonLine = (JSONObject) parser.parse(line);
                JSONObject jsonItem = (JSONObject) jsonLine.get("Item");
//                var item = jsonLine.getJSONObject("Item");

//                jsonLine.get("Item")
//                HashMap<String, AttributeValue> result = (HashMap<String, AttributeValue>) JacksonUtils.fromJsonString(line, HashMap.class).get("Item")
//                        .entrySet()
//                        .stream()
//                        .collect(Collectors.toMap(
//                                entry -> entry.toString(),
//                                entry -> entry
//                        ));

//                logger.log(jsonItem.toString() + "\n");
//                logger.log(jsonItem.getClass().toString() + "\n");

                WriteRequest item = WriteRequest.builder()
                        .putRequest(PutRequest.builder().item(jsonItem).build())
                        .build();

                itemList.add(item);

                logger.log("[" + batchCounter + "/" + counter + "] Adding item to itemlist \n");
                counter++;

                if (counter == BATCH_SIZE) {
                    batchCounter++;

                    var batchItemRequest = BatchWriteItemRequest.builder()
                            .requestItems(Map.of(DYNAMO_TABLE_NAME, itemList))
                            .build();

                    logger.log("Sending Batch "+ batchCounter +" \n");
//                    logger.log(batchItemRequest.toString() + "\n");

                    var outcome = dynamoDBClient.batchWriteItem(batchItemRequest);

                    do {
                        var unprocessedItemsRequest = BatchWriteItemRequest.builder()
                                .requestItems(outcome.unprocessedItems())
                                .build();

                        if (outcome.unprocessedItems().size() > 0) {
                            logger.log("Retrieving the unprocessed " + outcome.unprocessedItems().size() + " items, batch ["+ batchCounter+"].");
                            outcome = dynamoDBClient.batchWriteItem(unprocessedItemsRequest);
                        }

                    } while (outcome.unprocessedItems().size() > 0);
                    itemList.clear();
                    counter = 0;
                }
            }


            logger.log("Load finish in " + (System.currentTimeMillis() - startTime) + "ms");
            fileIn.close();
            gis.close();

            statusReport.setStatus(true);
        } catch (Exception ex) {
            logger.log(ex.getMessage());
        }

        statusReport.setExecutiongTime(System.currentTimeMillis() - startTime);
    }
}
