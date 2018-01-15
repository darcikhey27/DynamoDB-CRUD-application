package practice;

import java.util.*;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.EnvironmentVariableCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.DeleteItemSpec;
import com.amazonaws.services.dynamodbv2.document.utils.NameMap;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;
import com.amazonaws.services.dynamodbv2.model.*;


public class DynamoPractice {
    private AmazonDynamoDB client;
    //private AmazonDynamoDB dynamoDB;
    private DynamoDB dynamoDB;

    // TODO: connect to my Movies database
    public DynamoPractice() {
        // TODO: load aws credentials
        this.client = AmazonDynamoDBClientBuilder.standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("dynamodb.us-west-2.amazonaws.com", "us-west-2"))
                .build();
        this.dynamoDB = new DynamoDB(client);


        /*
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);

        Table table = dynamoDB.getTable("ProductCatalog");
         */ 
    }


    // todo: get an item from database
    public void getItem(String primaryPartitionKey, String keyName, String tableName) {
        HashMap<String, AttributeValue> keyToGet = new HashMap<String, AttributeValue>();
        keyToGet.put(primaryPartitionKey, new AttributeValue(keyName));

        GetItemRequest getItemRequest = new GetItemRequest().withKey(keyToGet).withTableName(tableName);

        try {
            Map<String, AttributeValue> returned_item = this.client.getItem(getItemRequest).getItem();
            if (returned_item != null) {
                Set<String> keys = returned_item.keySet();
                for (String key : keys) {
                    System.out.format("%s: %s\n", key, returned_item.get(key).toString());
                }
            } else {
                System.out.format("No item found with the key %s!\n", "The Civic");
            }
        } catch (AmazonServiceException e) {
            System.err.println(e.getErrorMessage());
            System.out.println("Error happend");
            System.exit(1);
        }

    }

    // todo: upload an item to dynamoDB
    public void putItem(String itemAttributes, String tableName) {
        //Table table = this.dynamoDB.getTable("ProductCatalog");
        Table table = dynamoDB.getTable(tableName);

        // Build the item
        Item item = new Item()
                .withPrimaryKey("Id", 123)
                .withString("Title", "Bicycle 123")
                .withString("Description", "123 description")
                .withString("BicycleType", "Hybrid")
                .withString("Brand", "Brand-Company C")
                .withNumber("Price", 500)
                .withStringSet("Color", new HashSet<String>(Arrays.asList("Red", "Black")))
                .withString("ProductCategory", "Bicycle")
                .withBoolean("InStock", true)
                .withNull("QuantityOnHand");

        // Write the item to the table
        PutItemOutcome outcome = table.putItem(item);

    }

    // todo: delete an item
    public void deleteItem(String itemName, String tableName) {
        Table table = dynamoDB.getTable(tableName);

        try {

            DeleteItemSpec deleteItemSpec = new DeleteItemSpec().withPrimaryKey("Id", 120)
                    .withConditionExpression("#ip = :val").withNameMap(new NameMap().with("#ip", "InPublication"))
                    .withValueMap(new ValueMap().withBoolean(":val", false)).withReturnValues(ReturnValue.ALL_OLD);

            DeleteItemOutcome outcome = table.deleteItem(deleteItemSpec);

            // Check the response.
            System.out.println("Printing item that was deleted...");
            System.out.println(outcome.getItem().toJSONPretty());

        } catch (Exception e) {
            System.err.println("Error deleting item in " + tableName);
            System.err.println(e.getMessage());
        }
    }

    public void createTable(String tableName) {
        //AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        CreateTableRequest request = new CreateTableRequest()
                .withAttributeDefinitions(new AttributeDefinition().withAttributeName("Artist").withAttributeType("S"),
                        new AttributeDefinition().withAttributeName("SongTitle").withAttributeType("S"))
                .withTableName("Music")
                .withKeySchema(new KeySchemaElement().withAttributeName("Artist").withKeyType("HASH"),
                        new KeySchemaElement().withAttributeName("SongTitle").withKeyType("RANGE"))
                .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(5L).withWriteCapacityUnits(5L));
        // use the already built DynamoDB object, or I can create an other object
        CreateTableResult response = this.client.createTable(request);
    }

    public static void main(String args[]) {
        DynamoPractice practice = new DynamoPractice();
        System.out.println("Good compile");

    }


    // todo: delete the Movies Table

}
