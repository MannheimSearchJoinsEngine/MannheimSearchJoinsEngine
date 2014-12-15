package de.mannheim.uni.mongodb;

import java.net.UnknownHostException;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;

/**
 * @author petar
 * 
 */
public class MongoDBManager {

	// db parameters
	public static final String DB_HOST = "localhost";
	public static final int DB_PORT = 27017;

	// using singleton pattern for the database connection
	private DB mongoDB;
	private static MongoDBManager instance;
	private MongoClient mongoClient;

	public MongoDBManager(String databaseName) {
		mongoDB = openDatabaseConnection(databaseName);
	}

	public static synchronized MongoDBManager getInstance(String databaseName) {
		if (instance == null) {
			instance = new MongoDBManager(databaseName);
		}
		return instance;
	}

	private DB openDatabaseConnection(String databaseName) {
		try {
			mongoClient = new MongoClient(DB_HOST, DB_PORT);
			mongoClient.setWriteConcern(WriteConcern.JOURNALED);
			DB db = mongoClient.getDB(databaseName);
			return db;
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public void closeDB() {
		mongoClient.close();
	}

	public DBCollection getDBCollection(String collectionName) {
		return mongoDB.getCollection(collectionName);
	}

	public void insertObjectInCollection(BasicDBObject object,
			DBCollection collection) {
		collection.insert(object);

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
