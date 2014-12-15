package de.mannheim.uni.mongodb;

import java.util.ArrayList;
import java.util.List;
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
public class CSV2MongoParallelWriter {
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

	private Pipeline pipeline;
	ConvertFileToTable fileToTable;
	private MongoDBManager mongoManager;

	public CSV2MongoParallelWriter(Pipeline pipeline) {
		this.pipeline = pipeline;
		fileToTable = new ConvertFileToTable(pipeline);
		mongoManager = MongoDBManager.getInstance(DB_NAME);
		// TODO Auto-generated constructor stub
	}

	public void insertFilesInMongo(String folderPath, ReadTableType tableType) {
		int cores = Runtime.getRuntime().availableProcessors();

		long start1 = System.currentTimeMillis();
		pipeline.getLogger().info("Analyzing folder " + folderPath);
		List<String> files = FileUtils.readFilesFromFolder(folderPath,
				pipeline.getMaxFileSize());
		pipeline.getLogger().info("Found " + files.size() + " files to insert");

		int base = files.size() / cores;
		List<String> filesList = new ArrayList<String>();
		int nmThreadsLeft = cores - 1;
		List<Thread> threads = new ArrayList<Thread>();
		for (String file : files) {
			filesList.add(file);
			if (filesList.size() > base && nmThreadsLeft > 0) {

				InsertFileThread thread = new InsertFileThread(pipeline,
						filesList, mongoManager, tableType);
				threads.add(thread);
				nmThreadsLeft--;
				filesList = new ArrayList<String>();
			}
		}

		InsertFileThread thread = new InsertFileThread(pipeline, filesList,
				mongoManager, tableType);

		threads.add(thread);

		for (Thread thready : threads) {
			thready.start();
		}
		// wait all threads to finish before closing the BD
		for (Thread thready : threads) {
			try {
				thready.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// close the connection
		mongoManager.closeDB();

		long end1 = System.currentTimeMillis();
		pipeline.getLogger().info(
				"Time inserting all tables: "
						+ ((double) (end1 - start1) / 1000));
	}

	private class InsertFileThread extends Thread {

		ConvertFileToTable fileToTable;
		private MongoDBManager mongoManager;
		private Pipeline pipeline;
		List<String> files;
		ReadTableType readTableType;

		public InsertFileThread(Pipeline pipeline, List<String> files,
				MongoDBManager mongoManager, ReadTableType readTableType) {
			this.pipeline = pipeline;
			this.mongoManager = mongoManager;
			this.files = files;
			fileToTable = new ConvertFileToTable(pipeline);
			this.readTableType = readTableType;
		}

		public void run() {
			for (String file : files) {
				long start = System.currentTimeMillis();
				pipeline.getLogger().info("Inserting " + file);

				// insert the file
				insertCSVinMongo(file, readTableType);

				long end = System.currentTimeMillis();
				pipeline.getLogger().info(
						"Time inserting the table: " + file
								+ ((double) (end - start) / 1000));
			}
		}

		private void insertCSVinMongo(String filePath, ReadTableType tableType) {
			Table table = null;
			table = fileToTable.readTable(tableType, filePath);

			convertTableToDBObject(table);

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
					String columnName = column.getHeader().replaceAll("\\.",
							"_");
					newObject.append(columnName,
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

	public BasicDBObject convertTableToDBObject(Table table) {
		if (table == null || table.getColumns() == null
				|| table.getColumns().size() == 0)
			return null;
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

		// set all columns
		insertColumnsInObject(doc, table);

		// insert the metaData
		DBCollection filesMeta = mongoManager
				.getDBCollection(DB_COLL_META_NAME);
		filesMeta.insert(doc);
		// pipeline.getLogger().info("Meta data inserted");
		// set all rows// we have to insert row by row because the document
		// has
		// a size limit
		// not inserting rows anymore
		// insertRows(table);

		return doc;
	}

	public void insertColumnsInObject(BasicDBObject doc, Table table) {
		BasicDBList array = new BasicDBList();

		for (TableColumn column : table.getColumns()) {
			String columnName = column.getHeader().replaceAll("\\.", "_");
			BasicDBObject colObj = new BasicDBObject(NAME, columnName).append(
					TYPE, column.getDataType().toString());
			array.add(colObj);
		}
		doc.append(COLUMNS, array);
	}
}
