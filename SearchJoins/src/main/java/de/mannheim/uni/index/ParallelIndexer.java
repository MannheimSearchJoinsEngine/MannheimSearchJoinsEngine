package de.mannheim.uni.index;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.utils.FileUtils;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.webtables.statistics.TableStatisticsExtractorLocal;

//TODO change the name of the class
/**
 * @author petar
 * 
 */
public class ParallelIndexer {

	private IndexManager indexManager = null;
	private AttributesIndexManager attributeIndexManager = null;
	private Pipeline pipeline;

	public ParallelIndexer(Pipeline pipeline) {
		this.pipeline = pipeline;
		this.indexManager = pipeline.getIndexManager();
		this.attributeIndexManager = pipeline.getAttributesIndexManager();
	}

	public void indexFilesFromFolder(String path, ReadTableType tableType)
			throws InterruptedException {
		List<String> files = FileUtils.readFilesFromFolder(path,
				pipeline.getMaxFileSize());

		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors(), Runtime.getRuntime()
				.availableProcessors(), 0, TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(files
						.size()));
		pipeline.getLogger().info("TOTAL DOCUMENTS TO INDEX :" + files.size());
		// read the table sequentally
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
		int tCount = 1;
		for (String file : files) {

			Table table = new Table();
			table.setFullPath(file);
			// table = fileToTable.readTable(tableType, file);
			// something is wrong with the table
			// if (table == null)
			// continue;
			IndexFileThread th = new IndexFileThread(pipeline, indexManager,
					table, fileToTable, tableType, attributeIndexManager);
			pipeline.getLogger().info("Indexing " + tCount + " table");
			tCount++;
			pool.execute(th);

		}

		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.DAYS);

	}

	public void indexFilesFromFolderByValuesParalel(String path,
			ReadTableType tableType) throws InterruptedException {
		List<String> files = FileUtils.readFilesFromFolder(path,
				pipeline.getMaxFileSize());
		ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 0,
				TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(
						files.size()));
		pipeline.getLogger().info("TOTAL DOCUMENTS TO INDEX :" + files.size());
		// read the table sequentally
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
		int tCount = 1;
		for (String file : files) {
			IndexFileThreadValue th = new IndexFileThreadValue(pipeline,
					fileToTable, file);

			pipeline.getLogger().info("Indexing " + tCount + " table");
			tCount++;
			pool.execute(th);

		}
		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.DAYS);

	}

	public void indexFilesFromFolderByValues(String path,
			ReadTableType tableType) throws InterruptedException {
		List<String> files = FileUtils.readFilesFromFolder(path,
				pipeline.getMaxFileSize());

		pipeline.getLogger().info("TOTAL DOCUMENTS TO INDEX :" + files.size());
		// read the table sequentally
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
		int tCount = 1;
		for (String file : files) {
			pipeline.getLogger().info("Indexing " + file);
			TableStats stat = fileToTable.readLODtableForIndexingByValue(file);
			pipeline.getIndexLogger().info(stat.getInfo());

			pipeline.getLogger().info("Indexing " + tCount + " table");
			tCount++;

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

			}

			try {
				indexFilesFromFolder((String) entry.getKey(),
						(ReadTableType) entry.getValue());
				// indexFilesFromFolderByValues((String) entry.getKey(),
				// (ReadTableType) entry.getValue());
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block

			}

		}
		try {
			attributeIndexManager.closeIndexWriter();
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

	static class IndexFileThreadValue implements Runnable {
		private String tablePath;
		private ConvertFileToTable fileToTable = null;
		private Pipeline pipeline;

		public IndexFileThreadValue(Pipeline pipeline,
				ConvertFileToTable fileToTable, String table) {
			tablePath = table;
			// TODO Auto-generated constructor stub
			this.pipeline = pipeline;
			this.fileToTable = fileToTable;
		}

		public void run() {
			// TODO Auto-generated method stub
			indexTable();
		}

		public void indexTable() {
			pipeline.getLogger().info("Indexing " + tablePath);
			TableStats stat = fileToTable
					.readLODtableForIndexingByValue(tablePath);
			pipeline.getIndexLogger().info(stat.getInfo());
		}
	}

	static class IndexFileThread implements Runnable {

		private IndexManager indexManager = null;
		private AttributesIndexManager attributeIndexManager = null;
		private Pipeline pipeline;
		private Table table;
		private ConvertFileToTable fileToTable;
		private ReadTableType typeTable;

		public IndexFileThread(Pipeline pipeline, IndexManager indexManager,
				Table table, ConvertFileToTable fileToTable,
				ReadTableType typeTable,
				AttributesIndexManager attributeIndexManager) {
			this.pipeline = pipeline;
			this.indexManager = indexManager;
			this.table = table;
			this.fileToTable = fileToTable;
			this.typeTable = typeTable;
			this.attributeIndexManager = attributeIndexManager;
		}

		public void run() {
			indexTable(table);

		}

		public void indexTable(Table table) {
			// use if it is a tar
			List<String> tablesToIndex = new ArrayList<String>();
			File unzipped = null;
			tablesToIndex.add(table.getFullPath());
			if (table.getFullPath().endsWith(".tar")) {
				try {
					unzipped = extractTarFile(table.getFullPath());
					tablesToIndex = new ArrayList<String>();
					for (File f : unzipped.listFiles()) {
						tablesToIndex.add(f.getAbsolutePath());
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
			} else if (table.getFullPath().endsWith(".tar.gz")) {
				try {
					unzipped = TableStatisticsExtractorLocal
							.extractTarFile(table.getFullPath());
					tablesToIndex = new ArrayList<String>();
					for (File f : unzipped.listFiles()) {
						if (f.getName().endsWith(".csv")
								&& !f.getName().contains("LINK"))
							tablesToIndex.add(f.getAbsolutePath());
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
			}
			for (String tablePath : tablesToIndex) {
				table = fileToTable.readTable(typeTable, tablePath);
				// something is wrong with the table
				if (table == null)
					continue;
				String tableName = table.getFullPath();
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
						continue;

					TableStats stat = new TableStats();
					stat.setHeader(table.getFullPath());
					stat.setNmCols(table.getColumns().size());
					stat.setNmRows(table.getColumns().get(0).getValues().size());

					pipeline.getLogger().info(
							"Indexing table:" + table.getFullPath());
					long start = System.currentTimeMillis();
					// if the user selected indexing the values
					if (pipeline.getIndexingMode() == 0
							|| pipeline.getIndexingMode() == 1) {
						if (pipeline.getKeyidentificationType() == KeyIdentificationType.single) {
							indexEntries = TableManager
									.getKeyIndexEntriesFromTable(table);
						} else if (pipeline.getKeyidentificationType() == KeyIdentificationType.none
								|| pipeline.getKeyidentificationType() == KeyIdentificationType.singleWithRefineAttrs) {
							indexEntries = TableManager
									.getIndexEntriesFromTable(table);
						}

						stat.setNmNulls(table.getNmNulls());
						//table = null;
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
					}

					// index the headers if needed
					if (pipeline.getIndexingMode() == 0
							|| pipeline.getIndexingMode() == 2) {
						List<ColumnIndexEntry> columnEntries = TableManager
								.getColumnsEntriesFromTable(table);
						for (ColumnIndexEntry entry : columnEntries) {
							try {
								attributeIndexManager.indexValue(entry);
							} catch (Exception e) {
								e.printStackTrace();
							}
						}
					}
					table = null;
					// write the stats to file
					pipeline.getIndexLogger().info(stat.getInfo());
					long end = System.currentTimeMillis();
					pipeline.getLogger().info(
							tableName + "The table was indexed for: "
									+ +((double) (end - start) / 1000));

				} catch (Exception e) {
					pipeline.getLogger().info(
							"Indexing table " + tableName + "failed");
					pipeline.getLogger().info(e.getMessage());
				}
				indexEntries = null;
			}
			if (unzipped != null) {
				try {
					org.apache.commons.io.FileUtils.deleteDirectory(unzipped);
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

	public static File extractTarFile(String path) throws Exception {
		/* Read TAR File into TarArchiveInputStream */
		TarArchiveInputStream myTarFile = new TarArchiveInputStream(
				new FileInputStream(new File(path)));

		String outputDirName = path.replaceAll(".tar$", "");
		File outputDir = new File(outputDirName);
		outputDir.mkdir();
		/* To read individual TAR file */
		TarArchiveEntry entry = null;
		String individualFiles;
		int offset;
		FileOutputStream outputFile = null;
		/* Create a loop to read every single entry in TAR file */
		while ((entry = myTarFile.getNextTarEntry()) != null) {
			/* Get the name of the file */
			individualFiles = entry.getName();
			/* Get Size of the file and create a byte array for the size */
			byte[] content = new byte[(int) entry.getSize()];
			offset = 0;
			/* Some SOP statements to check progress */
			// System.out.println("File Name in TAR File is: "
			// + individualFiles);
			// System.out.println("Size of the File is: " +
			// entry.getSize());
			// System.out.println("Byte Array length: " + content.length);
			/* Read file from the archive into byte array */
			myTarFile.read(content, offset, content.length - offset);
			/* Define OutputStream for writing the file */
			outputFile = new FileOutputStream(new File(outputDirName + "/"
					+ individualFiles));
			/* Use IOUtiles to write content of byte array to physical file */
			IOUtils.write(content, outputFile);
			/* Close Output Stream */
			outputFile.close();
		}
		/* Close TarAchiveInputStream */
		myTarFile.close();
		return outputDir;
	}

}
