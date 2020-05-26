package database;


import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
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
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import constants.DynamoConstants;
import constants.JsonConstants;
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
            		.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(DynamoConstants.LOCAL_HOST + ":" + DynamoConstants.DATABASE_PORT, DynamoConstants.DATABASE_REGION)).build();
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
	
	
	
	
	public boolean createTableMovies(String tableName) {
		
		boolean sucess = true;
		
        try {
            System.out.println("Attempting to create table; please wait...");
            CreateTableResult table = dynamoDB.createTable(
            			// 2 fields type string (ScalarAttributeType.S)
            			Arrays.asList(new AttributeDefinition("year", ScalarAttributeType.N), new AttributeDefinition("title", ScalarAttributeType.S)),
            			//Arrays.asList( new AttributeDefinition(KeyName, ScalarAttributeType.N) ),
            			tableName,
            			// Partition // key
            			Arrays.asList(new KeySchemaElement("year", KeyType.HASH),new KeySchemaElement("title", KeyType.RANGE)), // Sort key                
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
	
	
	
	public boolean cargaDatosTablaMovies() throws Exception, IOException {
		
		boolean loadSucess = true;
		
		AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
	            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:8000", "us-west-2"))
	            .build();

	        DynamoDB dynamoDB = new DynamoDB(client);

	        Table table = dynamoDB.getTable("Movies");
	        
	        // File's path
	        String filePath = System.getProperty("user.dir") + JsonConstants.MOVIES_FILE_PATH + "\\" + JsonConstants.MOVIES_FILE_NAME;
	        JsonParser parser = new JsonFactory().createParser(new File(filePath));

	        JsonNode rootNode = new ObjectMapper().readTree(parser);
	        Iterator<JsonNode> iter = rootNode.iterator();

	        ObjectNode currentNode;

	        while (iter.hasNext()) {
	            currentNode = (ObjectNode) iter.next();

	            int year = currentNode.path("year").asInt();
	            String title = currentNode.path("title").asText();

	            try {
	                table.putItem(new Item().withPrimaryKey("year", year, "title", title).withJSON("info", currentNode.path("info").toString()));
	                System.out.println("PutItem succeeded: " + year + " " + title);

	            }
	            catch (Exception e) {
	                System.err.println("Unable to add movie: " + year + " " + title);
	                System.err.println(e.getMessage());
	                break;
	            }
	        }
	        parser.close();
        
        return loadSucess;
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
