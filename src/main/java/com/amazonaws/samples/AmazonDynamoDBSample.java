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
    		String descriptionTable = dynamoDatabaseAccessImpl.describeTable("Prueba001");
    		LogWriter.writeLog(descriptionTable);
    	} else {
    		LogWriter.writeLog("Failure to initialice the database connection");
    	}
    	
    	
    	
    	
    	
    	//System.out.println("");
      
    }



}
