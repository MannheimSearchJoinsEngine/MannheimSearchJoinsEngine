package de.mannheim.uni.wikipedia;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.index.ParallelIndexer;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.FileUtils;

/**
 * @author petar
 * 
 */
public class WikiLinkLookup {

	public static void main(String[] args) {

		Pipeline pipe = Pipeline.getPipelineFromConfigFile(args[1], args[2]);
		WikiLinkLookup lo = new WikiLinkLookup(pipe);
		try {
			System.out.println("starting the process");
			lo.readWikiFromRepo(args[0]);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Pipeline pipeline;

	public WikiLinkLookup(Pipeline pipeline) {
		this.pipeline = pipeline;

	}

	public void readWikiFromRepo(String path) throws InterruptedException {
		System.out.println("READING FILES");
		List<String> files = FileUtils.readFilesFromFolder(path, 10000);
		System.out.println("FILES WERE LOADED");
		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors(), Runtime.getRuntime()
				.availableProcessors(), 0, TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(files
						.size()));
		pipeline.getLogger().info("TOTAL DOCUMENTS TO INDEX :" + files.size());

		int tCount = 1;
		for (String file : files) {

			WikiLookupThread th = new WikiLookupThread(pipeline, file);
			pipeline.getLogger().info("Indexing " + tCount + " table");
			tCount++;
			pool.execute(th);

		}

		pool.shutdown();
		pool.awaitTermination(10, TimeUnit.DAYS);

	}

	static class WikiLookupThread implements Runnable {
		private String tablePath;

		private Pipeline pipeline;

		public WikiLookupThread(Pipeline pipeline, String table) {
			tablePath = table;
			this.pipeline = pipeline;
		}

		public void run() {
			// TODO Auto-generated method stub
			checkTable();
		}

		private void checkTable() {
			List<String> tablesToIndex = new ArrayList<String>();
			File unzipped = null;
			tablesToIndex.add(tablePath);
			if (tablePath.endsWith(".tar")) {
				try {
					unzipped = ParallelIndexer.extractTarFile(tablePath);
					tablesToIndex = new ArrayList<String>();
					for (File f : unzipped.listFiles()) {
						tablesToIndex.add(f.getAbsolutePath());
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return;
				}
			}
			for (String tablePath : tablesToIndex) {
				long start = System.currentTimeMillis();
				pipeline.getLogger().info("checking table" + tablePath);
				String result = ConvertFileToTable
						.readwebTableWikiLookup(tablePath);
				if (!result.equals("")) {
					// write to logger
					pipeline.getIndexLogger().info(result);

					String newFileName = tablePath;
					if (newFileName.contains("/")) {
						newFileName = newFileName.substring(newFileName
								.lastIndexOf("/") + 1);
					}
					if (newFileName.contains("\\")) {
						newFileName = newFileName.substring(newFileName
								.lastIndexOf("\\") + 1);
					}
					try {
						org.apache.commons.io.FileUtils.copyFile(new File(
								tablePath), new File("/data/wikiTables/"
								+ newFileName));
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				pipeline.getLogger().info(
						tablePath + " was checked for: "
								+ (System.currentTimeMillis() - start));
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

}
