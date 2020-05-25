package com.amazonaws.samples;
import access.DynamoDatabaseAccessImpl;
import logs.LogWriter;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {

    //static AmazonDynamoDB dynamoDB;

    public static void main(String[] args) throws Exception {
    	
    	LogWriter.writeLog("Class constructor");
    	DynamoDatabaseAccessImpl dynamoDatabaseAccessImpl = new DynamoDatabaseAccessImpl();
    	
    	
    	LogWriter.writeLog("Initialize connection");
    	boolean sucessInitialDatabaseConnection = dynamoDatabaseAccessImpl.initConnexion();
    	
    	if (sucessInitialDatabaseConnection) {
    		
    		LogWriter.writeLog(dynamoDatabaseAccessImpl.listTables());
    		
    		
    		
    		///////////////////////////////////////////////////
    		// CREATE A NEW TABLE
    		///////////////////////////////////////////////////
    		//dynamoDatabaseAccessImpl.createTable("Prueba003", "Clave", "Valor");
    		//String descriptionTable = dynamoDatabaseAccessImpl.describeTable("Prueba003");
    		//LogWriter.writeLog(descriptionTable);
    		
    		
    		
    		
    		////////////////////////////////////////////////////
    		// ADD one element to above table created
    		////////////////////////////////////////////////////
    		//String contenidoTablaPrueba003 = "{\"name\":\"pepe\",\"apellido\":\"costas\",\"DNI\":\"03472188W\"}";
//    		String contenidoTablePrueba003 = "{\n" + 
//    				"  \"name\": \"pepe\",\n" + 
//    				"  \"apellido\": \"costas\",\n" + 
//    				"  \"DNI\": \"03472188W\"\n" + 
//    				"}";
//    		dynamoDatabaseAccessImpl.insertItemIntoTable("Prueba003", "AAA0003", contenidoTablePrueba003);
    		
    		
    		
    		
    		
    		//////////////////////////////////
    		// Delete table (Prueba003)
    		//////////////////////////////////
    		// dynamoDatabaseAccessImpl.deleteTable("Prueba003");
    		
    		
    		LogWriter.writeLog(dynamoDatabaseAccessImpl.listTables());
    		
    	} else {
    		LogWriter.writeLog("Failure to initialice the database connection");
    	}
    	
    	
    	
    	
    	
    	//System.out.println("");
      
    }



}
