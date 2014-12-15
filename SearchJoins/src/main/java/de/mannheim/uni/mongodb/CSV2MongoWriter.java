package de.mannheim.uni.mongodb;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.FileUtils;

/**
 * @author petar
 * 
 */
public class CSV2MongoWriter {
	// database authentication
	public static final String DB_NAME = "loddb";
	public static final String DB_COLL_META_NAME = "filesMeta";
	public static final String DB_COLL_DATA_NAME = "filesData";

	// mongo filed names
	public static final String DOC_ID = "ID";
	public static final String PRIMARY_KEY = "primaryKey";
	public static final String NM_COLS = "nmCols";
	public static final String NM_ROWS = "nmRows";
	public static final String COLUMNS = "columns";
	public static final String ROWS = "rows";
	public static final String NAME = "name";
	public static final String TYPE = "type";
	public static final String ROW_ID = "rowID";

	// meta data
	public static final String URL = "url";
	public static final String TLD = "tld";
	public static final String STARTING_POS = "startingPos";
	public static final String ENDING_POS = "endingPos";
	public static final String COLUMN_INDEX = "tableIndex";
	public static final String COLUMN_GRAPH_ID = "graphMappedID";

	private Pipeline pipeline;
	ConvertFileToTable fileToTable;
	private MongoDBManager mongoManager;

	private Map<String, Map<Integer, Integer>> mappedGraphColumnIds;

	public CSV2MongoWriter(Pipeline pipeline) {
		this.pipeline = pipeline;
		fileToTable = new ConvertFileToTable(pipeline);
		mongoManager = MongoDBManager.getInstance(DB_NAME);
		mappedGraphColumnIds = new HashMap<String, Map<Integer, Integer>>();
		// read the mapped graph ids
		getGraphMappedIds(pipeline.getGprahMappedIDFile());
		// TODO Auto-generated constructor stub
	}

	public void insertFilesInMongo(String folderPath, ReadTableType tableType) {
		long start1 = System.currentTimeMillis();

		pipeline.getLogger().info("Analyzing folder " + folderPath);
		List<String> files = FileUtils.readFilesFromFolder(folderPath,
				pipeline.getMaxFileSize());
		pipeline.getLogger().info("Found " + files.size() + " files to insert");
		for (String file : files) {
			long start = System.currentTimeMillis();
			pipeline.getLogger().info("Inserting " + file);

			// insert the file
			insertCSVinMongo(file, tableType);

			long end = System.currentTimeMillis();
			pipeline.getLogger().info(
					"Time inserting the table " + file + ": "
							+ +((double) (end - start) / 1000));
		}
		// close the connection
		mongoManager.closeDB();

		long end1 = System.currentTimeMillis();
		pipeline.getLogger().info(
				"Time inserting all tables "
						+ ((double) (end1 - start1) / 1000));
	}

	/**
	 * reads the file ids
	 * 
	 * @param gprahMappedIDFile
	 */
	private void getGraphMappedIds(String gprahMappedIDFile) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(gprahMappedIDFile));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";
			int i = 0;
			while ((line = br.readLine()) != null) {
				String[] vals = line.split("\t");
				Map<Integer, Integer> insideMap = new HashMap<Integer, Integer>();
				if (mappedGraphColumnIds.containsKey(vals[0])) {
					insideMap = mappedGraphColumnIds.get(vals[0]);
				}
				insideMap.put(Integer.parseInt(vals[1]),
						Integer.parseInt(vals[2]));
				mappedGraphColumnIds.put(vals[0], insideMap);
			}
			br.close();
		} catch (Exception e) {
			// TODO: handle exception
		}

	}

	public void closeMongoDBConnection() {
		mongoManager.closeDB();
	}

	private void insertCSVinMongo(String filePath, ReadTableType tableType) {
		Table table = null;
		table = fileToTable.readTable(tableType, filePath);

		convertTableToDBObject(table, null);

	}

	public BasicDBObject convertTableToDBObject(Table table, String[] metaData) {
		// set the ID of the document
		BasicDBObject doc = new BasicDBObject(DOC_ID, table.getFullPath());

		// set the primary key
		if (table.getCompaundKeyColumns().size() > 0) {
			BasicDBList array = new BasicDBList();

			for (TableColumn col : table.getCompaundKeyColumns())
				array.add(col.getHeader());
			doc.append(PRIMARY_KEY, array);
		}
		// set #cols
		doc.append(NM_COLS, table.getColumns().size());

		// set #rows
		doc.append(NM_ROWS, table.getColumns().get(0).getTotalSize());

		// read the meta data
		doc.append(URL, metaData[0].replaceFirst("\"", ""));
		doc.append(TLD, metaData[1]);
		doc.append(STARTING_POS, metaData[5]);
		doc.append(ENDING_POS, metaData[6].replace("\"", ""));

		// set all columns
		insertColumnsInObject(doc, table);

		// insert the metaData
		DBCollection filesMeta = mongoManager
				.getDBCollection(DB_COLL_META_NAME);
		filesMeta.insert(doc);
		pipeline.getLogger().info("Meta data inserted");
		// set all rows// we have to insert row by row because the document has
		// a size limit

		insertRows(table);

		return doc;
	}

	private void insertColumnsInObject(BasicDBObject doc, Table table) {
		BasicDBList array = new BasicDBList();

		int i = 0;
		for (TableColumn column : table.getColumns()) {
			BasicDBObject colObj = new BasicDBObject(NAME, column.getHeader())
					.append(TYPE, column.getDataType().toString()).append(
							COLUMN_INDEX, i);
			if (mappedGraphColumnIds.containsKey(table.getHeader())
					&& mappedGraphColumnIds.get(table.getHeader()).containsKey(
							i))
				colObj.append(COLUMN_GRAPH_ID,
						mappedGraphColumnIds.get(table.getHeader()).get(i));
			else
				colObj.append(COLUMN_GRAPH_ID, -1);
			array.add(colObj);
			i++;
		}
		doc.append(COLUMNS, array);
	}

	private void insertRows(Table table) {

		// open the data collection
		DBCollection filesData = mongoManager
				.getDBCollection(DB_COLL_DATA_NAME);

		List<DBObject> objectsTOInsert = new ArrayList<DBObject>();
		int rowCounter = 0;
		// iterate all rows
		for (Entry<Integer, String> entry : table.getColumns().get(0)
				.getValues().entrySet()) {

			// set the same ID as the file
			BasicDBObject newObject = new BasicDBObject(DOC_ID,
					table.getFullPath());

			// set the id of the column
			newObject.append(ROW_ID, entry.getKey());
			for (TableColumn column : table.getColumns()) {
				newObject.append(column.getHeader(),
						column.getValues().get(entry.getKey()));
			}
			objectsTOInsert.add(newObject);
			if (objectsTOInsert.size() % 1000 == 0) {
				// insert the documents
				filesData.insert(objectsTOInsert);
				objectsTOInsert = new ArrayList<DBObject>();
				System.out.println(rowCounter);
			}
			rowCounter++;
		}
		// insert the rest of the documents
		filesData.insert(objectsTOInsert);

	}
}
