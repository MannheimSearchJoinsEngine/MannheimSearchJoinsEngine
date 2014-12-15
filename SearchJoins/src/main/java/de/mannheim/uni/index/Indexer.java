package de.mannheim.uni.index;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.searchjoin.SearchJoin;
import de.mannheim.uni.utils.FileUtils;

//TODO change the name of the class
/**
 * @author petar
 * 
 */
public class Indexer {

	private IndexManager indexManager = null;
	private Pipeline pipeline;

	public Indexer(Pipeline pipeline) {
		this.pipeline = pipeline;
		this.indexManager = new IndexManager(pipeline);
	}

	public void addFilesToIndex(List<String> filesPath, ReadTableType tableType) {
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
		for (String filePath : filesPath) {
			pipeline.getLogger().info("Indexing table:" + filePath);
			long start = System.currentTimeMillis();
			Table table = null;
			table = fileToTable.readTable(tableType, filePath);

			List<IndexEntry> indexEntries = TableManager
					.getKeyIndexEntriesFromTable(table);
			if (pipeline.getKeyidentificationType() == KeyIdentificationType.none
					|| pipeline.getKeyidentificationType() == KeyIdentificationType.singleWithRefineAttrs)
				indexEntries = TableManager.getIndexEntriesFromTable(table);
			for (IndexEntry entry : indexEntries)
				indexManager.indexValue(entry);
			long end = System.currentTimeMillis();
			pipeline.getLogger().info(
					"The table was indexed for: "
							+ +((double) (end - start) / 1000));
		}

	}

	// public void addFilesToIndexInChunks(List<String> filesPath) {
	// ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
	// for (String filePath : filesPath) {
	// pipeline.getLogger().info("Indexing table:" + filePath);
	// long start = System.currentTimeMillis();
	// fileToTable.indexFileByColumns(filePath, indexManager);
	// long end = System.currentTimeMillis();
	// pipeline.getLogger().info(
	// "The table was indexed for: "
	// + ((double) (end - start) / 1000));
	// }
	// try {
	// indexManager.closeIndexWriter();
	// } catch (Exception e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	// }

	public void indexFilesFromFolder(String path, ReadTableType tableType) {

		List<String> files = FileUtils.readFilesFromFolder(path,
				pipeline.getMaxFileSize());

		addFilesToIndex(files, tableType);

	}

	public void indexRepos(Map<String, ReadTableType> repos) {

		long start = System.currentTimeMillis();
		for (Entry entry : repos.entrySet()) {
			try {
				pipeline.getLogger().info(
						"#Docs before indexing " + entry.getKey() + ": "
								+ Integer.toString(indexManager.getNmDocs()));
			} catch (Exception e) {
				e.printStackTrace();
			}

			indexFilesFromFolder((String) entry.getKey(),
					(ReadTableType) entry.getValue());

		}
		try {
			indexManager.closeIndexWriter();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pipeline.getLogger().info(
				"#Docs after indexing " + repos.keySet().iterator().next()
						+ ": " + Integer.toString(indexManager.getNmDocs()));

		long end = System.currentTimeMillis();

		pipeline.getLogger().info(
				"Time for indexing all tables: "
						+ ((double) (end - start) / 1000));

	}

	public static void main(String[] args) {
		// Indexer indexer = new Indexer();
		// indexer.indexFilesFromFolder("Output", false);
		// indexer.indexFilesFromFolder("Data/HeikoTestIndex", true);

		// IndexManager mng = new IndexManager();
		// List<IndexEntry> entries = mng.searchIndex("princeon university~");
		// for (IndexEntry entry : entries) {
		// entry.printEntry();
		// }

		// SearchJoin searchJoin = new SearchJoin();
		// searchJoin.searchJoinForTable("Data/HeikoQueryTest/QueryTable.csv");

		// ConvertFileToTable fileToTable = new ConvertFileToTable();
		// Table table = fileToTable
		// .readTableFromFileforIndexing("Data/DBpediaTables/University.csv");
		//
		// TableKeyIdentifier keyIdentifier = new TableKeyIdentifier();
		// keyIdentifier.identifyCompaundKey(table);
	}

}
