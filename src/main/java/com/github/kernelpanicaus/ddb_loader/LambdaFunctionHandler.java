package com.github.kernelpanicaus.ddb_loader;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.jayway.jsonpath.JsonPath;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse;
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
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.zip.GZIPInputStream;

/**
 * The Class LambdaFunctionHandler.
 * This application loads GZIPped Dynamo JSON file to DynamoDB.
 */
public class LambdaFunctionHandler implements RequestStreamHandler {

    /**
     * Provide the AWS region which your DynamoDB table is hosted.
     */
    private static final Region AWS_REGION = Region.of(System.getenv("AWS_REGION"));

    /**
     * The DynamoDB table name.
     */
    // TODO: Make this dynamic, from the S3 event.
    private static final String DYNAMO_TABLE_NAME = System.getenv("DYNAMO_TABLE_NAME");

    /**
     * Configurable batch size
     */
    private static final int BATCH_SIZE = Integer.parseInt(System.getenv().getOrDefault("BATCH_SIZE", "25"));
//
//    static final ClientConfiguration config = new ClientConfiguration()
//            .withMaxConnections(ClientConfiguration.DEFAULT_MAX_CONNECTIONS * 2);

    private static final S3Client s3Client = S3Client.builder()
            .region(AWS_REGION)
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build();

    private static final DynamoDbClient dynamoDBClient = DynamoDbClient.builder()
            .region(AWS_REGION)
            .httpClientBuilder(UrlConnectionHttpClient.builder())
            .build();

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

            ResponseInputStream<GetObjectResponse> responseInputStream = getS3ClientObject(srcKey, srcBucket);

            logger.log("Reading input stream \n");

            GZIPInputStream gis = new GZIPInputStream(responseInputStream);
            Scanner fileIn = new Scanner(gis);
            var parser = new JSONParser();

            int counter = 0;
            int batchCounter = 0;
            List<WriteRequest> itemList = new ArrayList<>();

            while (fileIn.hasNext()) {

                JSONObject jsonItem = getWriteItemRequest(fileIn, parser);

                itemList.add(getWriteItemRequest(jsonItem));

                logger.log("[" + batchCounter + "/" + counter + "] Adding item to itemlist \n");
                counter++;

                if (counter == BATCH_SIZE) {

                    logger.log("Sending Batch " + batchCounter + " \n");
                    BatchWriteItemResponse outcome = getBatchWriteItemResponse(Map.of(DYNAMO_TABLE_NAME, itemList));

                    do {
                        BatchWriteItemRequest unprocessedItemsRequest = getBatchWriteItemRequest(outcome.unprocessedItems());

                        if (outcome.unprocessedItems().size() > 0) {
                            logger.log("Retrieving the unprocessed " + outcome.unprocessedItems().size() + " items, batch [" + batchCounter + "].");
                            outcome = batchWrite(unprocessedItemsRequest);
                        }

                    } while (outcome.unprocessedItems().size() > 0);
                    itemList.clear();
                    batchCounter++;
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

    /**
     * <p>
     * Builds and returns a <code>WriteRequest</code> for JSON object.
     * </p>
     *
     * @param jsonItem
     * @return WriteRequest object
     */
    private WriteRequest getWriteItemRequest(JSONObject jsonItem) {
        return WriteRequest.builder()
                .putRequest(PutRequest.builder().item(jsonItem).build())
                .build();
    }

    /**
     * <p>
     * Takes a Map of Dynamo Table name to a List of Write requests and executes a bulk write.
     * </p>
     *
     * @param items Mapping of table name to collection of write requests.
     * @return BatchWriteItemResponse
     */
    private BatchWriteItemResponse getBatchWriteItemResponse(Map<String, List<WriteRequest>> items) {
        return batchWrite(getBatchWriteItemRequest(items));
    }

    /**
     * <p>
     * Builds and return a BatchWriteItemRequest object.
     * </p>
     *
     * @param items Mapping of table name to collection of write requests.
     * @return BatchWriteItemRequest
     */
    private BatchWriteItemRequest getBatchWriteItemRequest(Map<String, List<WriteRequest>> items) {
        return BatchWriteItemRequest.builder()
                .requestItems(items)
                .build();
    }

    /**
     * <p>
     * Executes BatchWriteItem operation against a Dynamo Table.
     * </p>
     *
     * @param `batchItemRequest`
     * @return <code>BatchWriteItemResponse</code>
     */
    private BatchWriteItemResponse batchWrite(BatchWriteItemRequest batchItemRequest) {
        return dynamoDBClient.batchWriteItem(batchItemRequest);
    }

    /**
     * <p>
     * Returns inner JSON object.
     * </p>
     *
     * @param fileIn
     * @param parser
     * @return <code>JSONObject</code>
     * @throws ParseException
     */
    private JSONObject getWriteItemRequest(Scanner fileIn, JSONParser parser) throws ParseException {
        var line = fileIn.nextLine();
        JSONObject jsonLine = (JSONObject) parser.parse(line);
        return (JSONObject) jsonLine.get("Item");
    }

    /**
     * <p>
     * getS3ClientObject and returns an S3 Object as a stream.
     * </p>
     *
     * @param srcKey
     * @param srcBucket
     * @return
     */
    private ResponseInputStream<GetObjectResponse> getS3ClientObject(String srcKey, String srcBucket) {
        GetObjectRequest objectRequest = GetObjectRequest.builder()
                .bucket(srcBucket)
                .key(srcKey)
                .build();

        return s3Client.getObject(objectRequest, ResponseTransformer.toInputStream());
    }
}
