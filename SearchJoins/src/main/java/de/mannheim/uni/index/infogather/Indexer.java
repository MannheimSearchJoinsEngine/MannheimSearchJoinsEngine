package de.mannheim.uni.index.infogather;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.mongodb.CSV2MongoWriter;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.FileUtils;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.webtables.ExtractedCSVReader;

public class Indexer {

	private KeyIndexManager indexManager = null;
	private Pipeline pipeline;
	private ConvertFileToTable fileToTable;
	private CSV2MongoWriter mongoWriter;
	private AttributesIndexManager attributeIndexManager = null;

	public static void main(String[] args) {
		Indexer in = new Indexer(args[0], args[1]);
		in.indexRepo(args[2]);
	}

	public Indexer(String pipeName, String confFile) {
		pipeline = Pipeline.getPipelineFromConfigFile(pipeName, confFile);
		indexManager = pipeline.getInfoGatherKeyIndexManager();
		fileToTable = new ConvertFileToTable(pipeline);
		mongoWriter = new CSV2MongoWriter(pipeline);
		attributeIndexManager = pipeline.getInfoGatherAttributeIndexManager();
	}

	public void indexRepo(String dataPath) {

		long start = System.currentTimeMillis();
		pipeline.getLogger().info("Start indexing tables!!!!");
		// read the three standard files
		Map<String, List<String>> dataFilesPaths = FileUtils
				.getFilePaths(dataPath);

		// index all files
		for (Entry<String, List<String>> entry : dataFilesPaths.entrySet()) {
			try {
				indexFilesFromArchive(entry.getValue());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		try {
			indexManager.closeIndexWriter();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			attributeIndexManager.closeIndexWriter();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		mongoWriter.closeMongoDBConnection();
		pipeline.getLogger().info(
				"Indexing all tables took:"
						+ (System.currentTimeMillis() - start));
	}

	private void indexFilesFromArchive(List<String> paths)
			throws InterruptedException {

		// store meta data to mongoDB
		Map<String, String[]> metaData = readMetaData(paths.get(2));

		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors(), Runtime.getRuntime()
				.availableProcessors(), 0, TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(Math.max(
						metaData.size(), 1)));
		ExtractedCSVReader csvReader = null;
		try {
			csvReader = new ExtractedCSVReader(paths.get(0));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			int tCount = 0;
			while (csvReader.next()) {

				IndexerThread th = new IndexerThread(indexManager, pipeline,
						fileToTable, ReadTableType.web, csvReader.getCSV(),
						csvReader.getFileName(), mongoWriter,
						metaData.get(csvReader.getFileName()),
						attributeIndexManager);
				pool.execute(th);

			}

			pool.shutdown();
			pool.awaitTermination(10, TimeUnit.DAYS);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private Map<String, String[]> readMetaData(String path) {
		Map<String, String[]> metaData = new HashMap<String, String[]>();
		BufferedReader br = null;
		List<String> files = new ArrayList<String>();
		try {
			int nm = 0;

			String sCurrentLine;

			br = new BufferedReader(new FileReader(path));

			while ((sCurrentLine = br.readLine()) != null) {
				String[] data = sCurrentLine.split("\",\"");
				metaData.put(data[2], data);

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		return metaData;
	}

	private class IndexerThread implements Runnable {
		private KeyIndexManager indexManager = null;
		private Pipeline pipeline;
		private String content;
		private ConvertFileToTable fileToTable;
		private ReadTableType typeTable;
		private String fileName;
		private CSV2MongoWriter mongoWriter;
		private String[] metaData;
		private AttributesIndexManager attributeIndexManager;

		public IndexerThread(KeyIndexManager indexManager, Pipeline pipeline,
				ConvertFileToTable fileToTable, ReadTableType typeTable,
				String content, String fileName, CSV2MongoWriter mongoWriter,
				String[] metaData, AttributesIndexManager attributeIndexManager) {
			super();
			this.indexManager = indexManager;
			this.pipeline = pipeline;
			this.fileToTable = fileToTable;
			this.typeTable = typeTable;
			this.content = content;
			this.fileName = fileName;
			this.mongoWriter = mongoWriter;
			this.metaData = metaData;
			this.attributeIndexManager = attributeIndexManager;
		}

		public void run() {
			// TODO Auto-generated method stub
			indexFile();
		}

		private void indexFile() {

			Table table = fileToTable.readTableFromString(content, fileName);
			// something is wrong with the table
			if (table == null)
				return;

			List<IndexEntry> indexEntries = null;
			try {
				// set the final types of each column
				for (TableColumn column : table.getColumns())
					column.setFinalDataType();

				// identify key
				TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(
						pipeline);
				keyIdentifier.identifyKey(table);
				if (!table.isHasKey())
					return;

				TableStats stat = new TableStats();
				stat.setHeader(table.getFullPath());
				stat.setNmCols(table.getColumns().size());
				stat.setNmRows(table.getColumns().get(0).getValues().size());

				pipeline.getLogger().info(
						"Indexing table:" + table.getFullPath());
				long start = System.currentTimeMillis();
				// get only the keys
				indexEntries = TableManager.getKeyIndexEntriesFromTable(table);

				stat.setNmNulls(table.getNmNulls());

				int eCount = 1;
				// index the entries
				for (IndexEntry entry : indexEntries) {
					if (!entry.getValue().equalsIgnoreCase(
							PipelineConfig.NULL_VALUE)
							&& !entry.getValue().equals(""))
						indexManager.indexValue(entry);
					// check if it is null
					if (entry.getValue().equalsIgnoreCase(
							PipelineConfig.NULL_VALUE)) {
						stat.setNmNulls(stat.getNmNulls() + 1);
					}
					if (eCount % 1000 == 0)
						System.out.println("Indexing " + eCount + "/"
								+ indexEntries.size());
					eCount++;
				}
				// write the stats to file
				pipeline.getIndexLogger().info(stat.getInfo());
				long end = System.currentTimeMillis();
				pipeline.getLogger().info(
						fileName + "The table was indexed for: "
								+ +((double) (end - start) / 1000));

			} catch (Exception e) {
				pipeline.getLogger().info(
						"Indexing table " + fileName + "failed");
				pipeline.getLogger().info(e.getMessage());
			}
			// write the attributtes in lucene
			List<ColumnIndexEntry> columnEntries = TableManager
					.getColumnsEntriesFromTable(table);
			for (ColumnIndexEntry entry : columnEntries) {
				try {
					attributeIndexManager.indexValue(entry);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			// write the data to mongoDB
			try {

				mongoWriter.convertTableToDBObject(table, metaData);
			} catch (Exception e) {
				pipeline.getLogger().info(
						"Writing table " + fileName + "to mongoDB failed");
				pipeline.getLogger().info(e.getMessage());
				e.printStackTrace();
			}
			indexEntries = null;
			table = null;
		}
	}

}
