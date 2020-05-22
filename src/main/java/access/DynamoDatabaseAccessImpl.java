package access;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import database.dynamo.Constants;

public class DynamoDatabaseAccessImpl implements DynamoDatabaseAccess {
	
	
	static AmazonDynamoDB dynamoDB;
	
	
	/**
	 * Constructor
	 */
	public DynamoDatabaseAccessImpl() {		
	}

	@Override
	public boolean initConnexion()  {
		
		boolean connectSucess = true;
        /*
         * The ProfileCredentialsProvider. reading from the credentials file
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
            
            dynamoDB = AmazonDynamoDBClientBuilder.standard()
            		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(Constants.LOCAL_HOST + ":" + Constants.DATABASE_PORT, Constants.DATABASE_REGION)).build();
        } catch (Exception e) {
        	connectSucess = false;
        	System.out.println("Cannot load the credentials from the credential profiles file, check the value elements un credentials file");
        	System.out.println(e);
        }
        
        return connectSucess;
    }

	@Override
	public boolean createTable(String tableName) {
		
		boolean sucess = true;
		
        try {
			CreateTableRequest createTableRequest = new CreateTableRequest().withTableName(tableName)
			        .withKeySchema(new KeySchemaElement().withAttributeName("name").withKeyType(KeyType.HASH))
			        .withAttributeDefinitions(new AttributeDefinition().withAttributeName("name").withAttributeType(ScalarAttributeType.S))
			        .withProvisionedThroughput(new ProvisionedThroughput().withReadCapacityUnits(1L).withWriteCapacityUnits(1L));
		} catch (Exception e) {
			sucess = false;
        	System.out.println("Error creating table " + tableName);
        	System.out.println(e);
		}
		return sucess;
	}

	@Override
	public String describeTable(String tableName) {
        DescribeTableRequest describeTableRequest = new DescribeTableRequest().withTableName(tableName);
        TableDescription tableDescription = dynamoDB.describeTable(describeTableRequest).getTable();
        return "Table Description: " + tableDescription;
	}

	@Override
	public boolean insertItemIntoTable(String tableName)  {
        Map<String, AttributeValue> item = newItem("Bill & Ted's Excellent Adventure", 1989, "****", "James", "Sara");
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        System.out.println("Result: " + putItemResult);
		return false;
	}
	
	
    private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("rating", new AttributeValue(rating));
        item.put("fans", new AttributeValue().withSS(fans));

        return item;
    }

}
