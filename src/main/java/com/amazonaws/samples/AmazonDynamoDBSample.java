package com.amazonaws.samples;
import access.DynamoDatabaseAccessImpl;

/**
 * This sample demonstrates how to perform a few simple operations with the
 * Amazon DynamoDB service.
 */
public class AmazonDynamoDBSample {

    //static AmazonDynamoDB dynamoDB;

    public static void main(String[] args) throws Exception {
    	
    	System.out.println("Class constructor");
    	DynamoDatabaseAccessImpl dynamoDatabaseAccessImpl = new DynamoDatabaseAccessImpl();
    	
    	
    	System.out.println("Initialize connection");
    	boolean sucessInitialDatabaseConnection = dynamoDatabaseAccessImpl.initConnexion();
    	
    	if (sucessInitialDatabaseConnection) {
    		String descriptionTable = dynamoDatabaseAccessImpl.describeTable("Prueba001");
    		System.out.println(descriptionTable);
    	} else {
    		System.out.println("Failure to initialice the database connection");
    	}
    	
    	
    	
    	
    	
    	//System.out.println("");
      
    }



}
