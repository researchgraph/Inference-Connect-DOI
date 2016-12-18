package org.researchgraph.app;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.StringUtils;
import org.researchgraph.configuration.Properties;
import org.researchgraph.connector.Connector;

public class App {
	public static void main(String[] args) {
		try {
			Class.forName("com.mysql.jdbc.Driver").newInstance();
			
			Configuration properties = Properties.fromArgs(args);
	        
	        String neo4jFolder = properties.getString(Properties.PROPERTY_NEO4J_FOLDER);
	        if (StringUtils.isEmpty(neo4jFolder))
	            throw new IllegalArgumentException("Neo4j Folder can not be empty");
	        System.out.println("Neo4J: " + neo4jFolder);
	          
	        String mysqlHost = properties.getString(Properties.PROPERTY_MYSQL_HOST);
	        int mysqlPort = properties.getInt(Properties.DEFAULT_MYSQL_PORT);
	        String mysqlUser = properties.getString(Properties.PROPERTY_MYSQL_USER);
	        String mysqlPassword = properties.getString(Properties.PROPERTY_MYSQL_PASSWORD);
	        String mysqlDatabase = properties.getString(Properties.PROPERTY_MYSQL_DATABASE);
	        
	        System.out.println("MySQL: " + mysqlHost);
	        
	        String source = properties.getString(Properties.PROPERTY_SOURCE_NAME);
	        String property = properties.getString(Properties.PROPERTY_PROPERTY_NAME);
	        String relationship = properties.getString(Properties.PROPERTY_RELATIONSHIP_NAME);
	        	        
	        System.out.println("Source: " + source); 
	        System.out.println("Property: " + property);
	        System.out.println("Relationship: " + relationship);
	        
	        Connector connector = new Connector(neo4jFolder, 
	        		mysqlHost, mysqlPort, mysqlUser, mysqlPassword, mysqlDatabase);
	        
	        connector.processNodes(source, property, relationship);
	        
		} catch (Exception e) {
            e.printStackTrace();
            
            System.exit(1);
		}       
	}
}
