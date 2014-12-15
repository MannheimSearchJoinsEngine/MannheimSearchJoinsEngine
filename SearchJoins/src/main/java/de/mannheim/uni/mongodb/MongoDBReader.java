package de.mannheim.uni.mongodb;

import java.util.LinkedList;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import de.mannheim.uni.index.IValueIndex;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.statistics.Timer;

/**
 * @author petar
 * 
 */
public class MongoDBReader implements IValueIndex {

	/*
	 * Missing functions:
	 * 
	 * get number of rows for a given table name get number of columns for a
	 * given table name get graph id given a table name and column index (use
	 * the graph_lookup file) get a JoinResult object given a Table object
	 * (query table) and a table name (entity table)
	 */

	private MongoDBManager mongoManager;

	public MongoDBReader() {
		mongoManager = MongoDBManager.getInstance(CSV2MongoWriter.DB_NAME);
		// TODO Auto-generated constructor stub
	}

	/**
	 * retrieves meta information about the file by its ID. Returns null if
	 * there is no such document
	 * 
	 * @param documentId
	 * @return
	 */
	public DBObject getFileMetaById(String documentId) {

		// open the collection
		DBCollection filesMetaCollection = mongoManager
				.getDBCollection(CSV2MongoWriter.DB_COLL_META_NAME);

		// set the query
		BasicDBObject query = new BasicDBObject(CSV2MongoWriter.DOC_ID,
				documentId);

		DBCursor cursor = filesMetaCollection.find(query);
		if (cursor.hasNext())
			return cursor.next();
		return null;
	}

	/**
	 * get number of rows for a csv file from mongoDB
	 * 
	 * @param documentId
	 * @return
	 */
	public int getRowNumberForFile(String documentId) {
		DBObject tableOb = getFileMetaById(documentId);
		return Integer
				.parseInt(tableOb.get(CSV2MongoWriter.NM_ROWS).toString());

	}

	/**
	 * get number of columns for a csv file from mongoDB
	 * 
	 * @param documentId
	 * @return
	 */
	public int getColumnNumberForFile(String documentId) {
		DBObject tableOb = getFileMetaById(documentId);
		return Integer
				.parseInt(tableOb.get(CSV2MongoWriter.NM_COLS).toString());

	}

	/**
	 * get the coresponding mapped graph id for the given column
	 * 
	 * @param documentId
	 * @return
	 */
	public int getGraphIndexNumberForFile(String documentId, int columnIndex) {
		Timer tim = Timer.getNamed("MongoDB.getGraphIndexNumber", null);
		// open the collection
		DBCollection filesMetaCollection = mongoManager
				.getDBCollection(CSV2MongoWriter.DB_COLL_META_NAME);

		// set the query
		BasicDBObject query = new BasicDBObject(CSV2MongoWriter.DOC_ID,
				documentId);
		// se tht index
		query.put(CSV2MongoWriter.COLUMNS + "." + CSV2MongoWriter.COLUMN_INDEX,
				columnIndex);
		// retrieve only the columns
		BasicDBObject fields = new BasicDBObject(
				CSV2MongoWriter.COLUMNS + ".$", 1);

		DBCursor cursor = filesMetaCollection.find(query, fields);
		if (cursor.hasNext()) {
			// get the first object and retrieve the id
			BasicDBList list = (BasicDBList) cursor.next().get(
					CSV2MongoWriter.COLUMNS);
			DBObject obj = (DBObject) list.get(0);
			
			tim.stop();
			return (Integer) obj.get(CSV2MongoWriter.COLUMN_GRAPH_ID);

		}

		tim.stop();
		return -1;

	}

	/**
	 * retrieves data row by file ID and row id. Returns null if there is no
	 * such document
	 * 
	 * @param documentId
	 * @param rowId
	 * @param columnName
	 * @return
	 */
	public List<DBObject> getFileRowById(String documentId, int rowId,
			String columnName) {

		// open the collection
		DBCollection filesDataCollection = mongoManager
				.getDBCollection(CSV2MongoWriter.DB_COLL_DATA_NAME);

		// set the query
		BasicDBObject query = new BasicDBObject(CSV2MongoWriter.DOC_ID,
				documentId);
		// add filter by row id
		if (rowId > -1) {
			query.append(CSV2MongoWriter.ROW_ID, rowId);
		}
		// add filter by column name
		if (columnName != null) {
			query.append(columnName, new BasicDBObject("$exists", true));
		}

		DBCursor cursor = filesDataCollection.find(query);
		List<DBObject> results = new LinkedList<DBObject>();
		while (cursor.hasNext()) {
			results.add(cursor.next());
		}
		return results;
	}

	public boolean hasHeader(String tableName, List<String> allowedHeaders)
	{
		return true;
	}
	
	public List<IndexEntry> getRowValues(String tabelHeader, int entryID) {
		return getRowValues(tabelHeader, entryID, null);
	}
	
	/**
	 * returns indexEntries from the row values
	 * 
	 * @param tabelHeader
	 * @param entryID
	 * @return
	 */
	public List<IndexEntry> getRowValues(String tabelHeader, int entryID, List<String> allowedHeaders) {
		List<IndexEntry> entries = new LinkedList<IndexEntry>();
		DBObject tableOb = getFileMetaById(tabelHeader);
		// get the column data
		DBObject columnsData = getFileRowById(tabelHeader, entryID, null)
				.get(0);
		// get all columns
		BasicDBList list = (BasicDBList) tableOb.get(CSV2MongoWriter.COLUMNS);
		String keyColumnName = ((BasicDBList) tableOb
				.get(CSV2MongoWriter.PRIMARY_KEY)).get(0).toString();
		for (int i = 0; i < list.size(); i++) {
			DBObject obj = (DBObject) list.get(i);

			// create the indexEntry
			IndexEntry entry = new IndexEntry();
			entry.setLuceneScore(0);
			entry.setEntryID(entryID);
			entry.setColumnDataType((String) obj.get(CSV2MongoWriter.TYPE));
			entry.setColumnHeader((String) obj.get(CSV2MongoWriter.NAME));
			entry.setColumnDistinctValues(-1);
			entry.setTabelHeader(tabelHeader);
			entry.setTableCardinality(-1);
			entry.setValueMultiplicity(-1);
			entry.setValue((String) columnsData.get((String) obj
					.get(CSV2MongoWriter.NAME)));
			entry.setFullTablePath(tabelHeader);
			entry.setOriginalValue((String) columnsData.get((String) obj
					.get(CSV2MongoWriter.NAME)));

			if (((String) obj.get(CSV2MongoWriter.NAME)).equals(keyColumnName)) {
				entry.setPrimaryKey(true);
			} else
				entry.setPrimaryKey(false);
			entries.add(entry);
		}

		// return entry;
		return entries;
	}

	public void printAllDocs() {
		DBCollection filesMetaCollection = mongoManager
				.getDBCollection(CSV2MongoWriter.DB_COLL_META_NAME);

		DBCursor cursor = filesMetaCollection.find();
		while (cursor.hasNext())
			System.out.println(cursor.next().toString());
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MongoDBReader reader = new MongoDBReader();
		// reader.printAllDocs();
		// DBObject obj1 = reader
		// .getFileMetaById("45068475_1_6553777529731426350.csv");
		// System.out.println(obj1.toString());
		// System.out.println(reader.getGraphIndexNumberForFile(
		// "45068475_1_6553777529731426350.csv", 1));
		// List<DBObject> objs = reader.getFileRowById(
		// "45068475_1_6553777529731426350.csv", 2, null);
		// //
		// for (DBObject obj : objs)
		// System.out.println(obj.toString());

		List<IndexEntry> entries = reader.getRowValues(
				"45068475_1_6553777529731426350.csv", 3);

		System.out.println("done");
	}
	/*
	 * THE META DATA JSON SCHEMA { "type": "object", "$schema":
	 * "http://json-schema.org/draft-03/schema", "id": "http://jsonschema.net",
	 * "required": false, "properties": { "ID": { "type": "string",
	 * "description": "The csv file name", "id": "http://jsonschema.net/ID",
	 * "required": false }, "_id": { "type": "number", "description":
	 * "automatically generated ID, should not be used", "id":
	 * "http://jsonschema.net/_id", "required": false }, "columns": { "type":
	 * "array", "id": "http://jsonschema.net/columns", "required": false,
	 * "items": { "type": "object", "id": "http://jsonschema.net/columns/0",
	 * "required": false, "properties": { "name": { "type": "string",
	 * "description": "name of the column", "id":
	 * "http://jsonschema.net/columns/0/name", "required": false }, "type": {
	 * "type": "string", "description": "type of the column", "id":
	 * "http://jsonschema.net/columns/0/type", "required": false },
	 * "tableIndex": { "type": "number", "description":
	 * "the position of the column inside the table (Starting with 0)", "id":
	 * "http://jsonschema.net/columns/0/type", "required": false },
	 * "graphMappedID": { "type": "number", "description":
	 * "the mapped id for the column inside the graph", "id":
	 * "http://jsonschema.net/columns/0/type", "required": false } } } },
	 * "endingPos": { "type": "string", "description":
	 * "The endingposition of the table inside the html ", "id":
	 * "http://jsonschema.net/endingPos", "required": false }, "nmCols": {
	 * "type": "number", "description": "number of columns in the file", "id":
	 * "http://jsonschema.net/nmCols", "required": false }, "nmRows": { "type":
	 * "number", "description": "The number of rows inside the table", "id":
	 * "http://jsonschema.net/nmRows", "required": false }, "primaryKey": {
	 * "type": "array", "description": "list of all primary key columns", "id":
	 * "http://jsonschema.net/primaryKey", "required": false, "items": { "type":
	 * "string", "id": "http://jsonschema.net/primaryKey/0", "required": false }
	 * }, "startingPos": { "type": "string", "description":
	 * "The starting position of the table inside the html ", "id":
	 * "http://jsonschema.net/s tartingPos", "required": false }, "tld": {
	 * "type": "string", "description": "the tld of the web page", "id":
	 * "http://jsonschema.net/tld", "required": false }, "url": { "type":
	 * "string", "description": "the url of the web page", "id":
	 * "http://jsonschema.net/url", "required": false } } }
	 */
}
