package access;


import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.CreateTableResult;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ListTablesResult;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.PutItemResult;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import database.dynamo.Constants;
import logs.LogWriter;

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
        	LogWriter.writeLog("Cannot load credentials from the credential profiles file, check the value elements un credentials file");
        	LogWriter.writeLog(e.toString());
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
			LogWriter.writeLog("Table created : " + tableName);
		} catch (Exception e) {
			sucess = false;
        	LogWriter.writeLog("Error creating table " + tableName);
        	LogWriter.writeLog(e.toString());
		}
		return sucess;
	}
	
	

	/**
	 * Creates a new table
	 * 
	 * @param tableName			name of the new table
	 * @param KeyName			key of the new table
	 * @param contenidoName		name of the field for data
	 * @return					boolean (sucess / fail)
	 */
	public boolean createTable(String tableName, String KeyName, String contenidoName) {
		
		boolean sucess = true;
		
        try {
            System.out.println("Attempting to create table; please wait...");
            CreateTableResult table = dynamoDB.createTable(
            			// 2 fields type string (ScalarAttributeType.S)
            			Arrays.asList(new AttributeDefinition(KeyName, ScalarAttributeType.S), new AttributeDefinition(contenidoName, ScalarAttributeType.S)),
            			//Arrays.asList( new AttributeDefinition(KeyName, ScalarAttributeType.N) ),
            			tableName,
            			// Partition // key
            			Arrays.asList(new KeySchemaElement(KeyName, KeyType.HASH),new KeySchemaElement(contenidoName, KeyType.RANGE)), // Sort key                
            			//Arrays.asList( new KeySchemaElement(KeyName, KeyType.HASH) ), // Sort key
            			new ProvisionedThroughput(10L, 10L)
            	);

            System.out.println("Success.  Table status: " + table.getTableDescription().toString());
			LogWriter.writeLog("Table created : " + tableName);
		} catch (Exception e) {
			sucess = false;
        	LogWriter.writeLog("Error creating table " + tableName);
        	LogWriter.writeLog(e.toString());
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
	/**
	 * Inserts into Prueba001
	 */
	public boolean insertItemIntoTable(String tableName)  {
        Map<String, AttributeValue> item = newItem("Bill & Ted's Excellent Adventure", 1989, "****", "James", "Sara");
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        System.out.println("Result: " + putItemResult);
		return false;
	}
	
	/**
	 * Inserts into Prueba003
	 */	
	public boolean insertItemIntoTable(String tableName, String key, String value)  {
        Map<String, AttributeValue> item = newItem(key, value);
        PutItemRequest putItemRequest = new PutItemRequest(tableName, item);
        PutItemResult putItemResult = dynamoDB.putItem(putItemRequest);
        System.out.println("Result: " + putItemResult);
		return false;
	}
	

	public String listTables() {
		ListTablesResult listTables = dynamoDB.listTables();
		List tablesNames = (List) listTables.getTableNames();
		return "Listado de tablas " + tablesNames.toString();
	}
	
	
    public static void deleteTable(String tableName) {        
        try {
            System.out.println("Issuing DeleteTable request for " + tableName);
            dynamoDB.deleteTable(tableName);
            System.out.println("Waiting for " + tableName + " to be deleted...");
        } catch (Exception e) {
            System.err.println("DeleteTable request failed for " + tableName);
            System.err.println(e.getMessage());
        }
    }
	
	
	/**
	 * For Prueba001
	 * @param name
	 * @param year
	 * @param rating
	 * @param fans
	 * @return
	 */
    private static Map<String, AttributeValue> newItem(String name, int year, String rating, String... fans) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("name", new AttributeValue(name));
        item.put("year", new AttributeValue().withN(Integer.toString(year)));
        item.put("rating", new AttributeValue(rating));
        item.put("fans", new AttributeValue().withSS(fans));

        return item;
    }
    
	/**
	 * For Prueba003
	 * 
	 * @param name
	 * @param year
	 * @param rating
	 * @param fans
	 * @return Map
	 */
    private static Map<String, AttributeValue> newItem(String key, String value) {
        Map<String, AttributeValue> item = new HashMap<String, AttributeValue>();
        item.put("Clave", new AttributeValue(key));
        item.put("Valor", new AttributeValue(value));
        return item;
    }

}
