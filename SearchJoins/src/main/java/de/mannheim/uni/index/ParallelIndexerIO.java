package de.mannheim.uni.index;

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
import de.mannheim.uni.utils.FileUtils;

//TODO change the name of the class
/**
 * @author petar
 * 
 */
public class ParallelIndexerIO {

	private IndexManager indexManager = null;
	private Pipeline pipeline;

	public ParallelIndexerIO(Pipeline pipeline) {
		this.pipeline = pipeline;
		this.indexManager = new IndexManager(pipeline);
	}

	public void indexFilesFromFolder(String path, ReadTableType tableType) {

		int cores = Runtime.getRuntime().availableProcessors();

		List<String> files = FileUtils.readFilesFromFolder(path,
				pipeline.getMaxFileSize());
		pipeline.getLogger().info("TOTAL DOCUMENTS TO INDEX :" + files.size());
		int base = files.size() / cores;
		List<String> filesList = new ArrayList<String>();
		int nmThreadsLeft = cores - 1;
		List<Thread> threads = new ArrayList<Thread>();
		for (String file : files) {
			filesList.add(file);
			if (filesList.size() > base && nmThreadsLeft > 0) {

				IndexFileThread thread = new IndexFileThread(pipeline,
						indexManager, tableType, filesList);
				threads.add(thread);
				nmThreadsLeft--;
				filesList = new ArrayList<String>();
			}
		}

		IndexFileThread thread = new IndexFileThread(pipeline, indexManager,
				tableType, filesList);

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

	static class IndexFileThread extends Thread {

		private IndexManager indexManager = null;
		private Pipeline pipeline;
		List<String> files;
		ReadTableType readTableType;

		public IndexFileThread(Pipeline pipeline, IndexManager indexManager,
				ReadTableType readTableType, List<String> files) {
			this.pipeline = pipeline;
			this.indexManager = indexManager;
			this.files = files;
			this.readTableType = readTableType;
		}

		public void run() {
			addFilesToIndex();

		}

		public void addFilesToIndex() {
			ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
			for (String filePath : files) {
				pipeline.getLogger().info("Indexing table:" + filePath);
				long start = System.currentTimeMillis();
				Table table = null;
				table = fileToTable.readTable(readTableType, filePath);

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
	}

}
