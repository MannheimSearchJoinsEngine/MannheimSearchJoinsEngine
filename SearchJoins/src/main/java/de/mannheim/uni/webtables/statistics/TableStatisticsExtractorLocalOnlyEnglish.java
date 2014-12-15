package de.mannheim.uni.webtables.statistics;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarOutputStream;


import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.DomainUtils;
import de.mannheim.uni.utils.FileUtils;
import de.mannheim.uni.utils.aws.QueueManager;
import de.mannheim.uni.utils.aws.S3Helper;
import de.mannheim.uni.utils.concurrent.SyncFileWriter;

public class TableStatisticsExtractorLocalOnlyEnglish implements Runnable {

	public static final int MIN_COLS = 3;
	public static final int MIN_ROWS = 5;
	public static final String[] ALLOWED_TLDS = new String[] { "com", "net",
			"org", "uk", "eu" };

	private String targetFolder;
	private String nonEnglishTargetFolder;

	private String engFilesTargetFolder;

	public String getEngFilesTargetFolder() {
		return engFilesTargetFolder;
	}

	public void setEngFilesTargetFolder(String engFilesTargetFolder) {
		this.engFilesTargetFolder = engFilesTargetFolder;
	}

	// private String fileName;
	// private String s3Bucket;
	// private S3Helper s3;

	private String s3OutputBucket;

	private static java.util.concurrent.atomic.AtomicLong counter = new AtomicLong();
	private static long total;

	private static long startTime;

	private SyncFileWriter doneWriter;

	private ConvertFileToTable fileToTable;

	public void setFileToTable(ConvertFileToTable fileToTable) {
		this.fileToTable = fileToTable;
	}

	private int totalFiles;

	private int processedFiles;

	private List<String> threadFiles;

	// used for the logger only
	private Pipeline pipeline;

	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	public void setTargetFolder(String targetFolder) {
		this.targetFolder = targetFolder;
	}

	public String getTargetFolder() {
		return targetFolder;
	}

	public void setThreadFiles(List<String> threadFiles) {
		this.threadFiles = threadFiles;
	}

	public List<String> getThreadFiles() {
		return threadFiles;
	};

	public void setDoneWriter(SyncFileWriter doneWriter) {
		this.doneWriter = doneWriter;
	}

	private QueueManager qm;

	public void setQueueManager(QueueManager queueManager) {
		qm = queueManager;
	}

	protected static void setTotal(long t) {
		total = t;
	}

	protected static double progressOne() {
		long current = counter.incrementAndGet();

		return (double) current / (double) total;
	}

	public void init(String fileName, String s3Bucket, S3Helper s3,
			String targetFolder, String s3OutputBucket,
			String nonEnglishTargetFolder) {

		this.targetFolder = targetFolder;
		// this.fileName = fileName;
		// this.s3Bucket = s3Bucket;
		// this.s3 = s3;
		this.s3OutputBucket = s3OutputBucket;
		this.nonEnglishTargetFolder = nonEnglishTargetFolder;

		this.fileToTable = new ConvertFileToTable(new Pipeline("", ""));

		File f = new File(targetFolder);
		if (!f.exists())
			f.mkdir();

		f = new File(nonEnglishTargetFolder);
		if (!f.exists())
			f.mkdir();
	}

	public static void main(String[] args) throws IOException {
		String targetFolder = "C:/Users/petar/Documents/ProjectsFiles/SearchJoins/FilesToIndex";
		String sourceFolder = "C:/Users/petar/Documents/ProjectsFiles/SearchJoins/WebTables/stats/";
		// TableExtractor t = new TableExtractor();
		// t.extractTables(sourceFolder, targetFolder);
		// extractTables(args[0], args[1]);
		// extractTables(
		// "C:/Users/petar/Documents/ProjectsFiles/SearchJoins/WebTables/webExtraction2",
		// "C:/Users/petar/Documents/ProjectsFiles/SearchJoins/WebTables/eng/",
		// "");
		if (args.length == 3)
			extractTables(args[0], args[1], args[2]);
		else
			extractTables(args[0], args[1], "");
	}

	public static void extractTables(String folderPath, String engFilesFolder,
			String filterFile) throws IOException {
		// this.targetFolder = targetFolder;

		// String s3Bucket = "WebTablesExtraction";
		// String s3OutputBucket = "SearchJoin-tables";
		// String s3AccessKey = "AKIAJUYAKM5IVKM2RJHA";
		// String s3SecretKey = "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";
		// String queueEndpoint = "sqs.us-east-1.amazonaws.com";
		// String queueName = "SearchJoins";
		//
		// S3Helper s3 = new S3Helper(s3AccessKey, s3SecretKey);

		// BufferedReader listReader = new BufferedReader(new
		// FileReader(fileList));

		// HashSet<String> files = new HashSet<String>();

		/*
		 * String file=null;
		 * 
		 * while((file = listReader.readLine()) != null) { files.add(file); }
		 * listReader.close();
		 * 
		 * BufferedReader doneReader = new BufferedReader(new
		 * FileReader("done.txt")); while((file = doneReader.readLine()) !=
		 * null) { if(files.contains(file)) files.remove(file); }
		 * doneReader.close();
		 * 
		 * setTotal(files.size());
		 */

		/*
		 * ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 0,
		 * TimeUnit.SECONDS, new
		 * java.util.concurrent.ArrayBlockingQueue<Runnable>( files.size()));
		 */

		startTime = System.currentTimeMillis();

		// SyncFileWriter w = new SyncFileWriter("done.txt", true);

		// QueueManager q = new QueueManager(s3AccessKey, s3SecretKey,
		// queueEndpoint, queueName);

		// divide files per thread
		int numProcessors = Runtime.getRuntime().availableProcessors();
		// numProcessors -= 4;
		List<List<String>> filesPerThread = new ArrayList<List<String>>();
		List<String> allFiles = new ArrayList<String>();
		if (filterFile == null || filterFile.equals("")) {
			allFiles = FileUtils.readFilesFromFolder(folderPath, 100000000);
		} else {
			List<String> allFilesTmp = org.apache.commons.io.FileUtils
					.readLines(new File(filterFile));
			// add the source path
			for (String str : allFilesTmp) {
				allFiles.add(folderPath + str);
			}
		}

		int maxfilesPerThread = allFiles.size() / numProcessors;

		int start = 0;
		for (int i = 0; i < numProcessors - 1; i++) {
			List<String> currentFiles = new ArrayList<String>();
			currentFiles.addAll((allFiles.subList(start, maxfilesPerThread
					* (i + 1))));
			start = maxfilesPerThread * (i + 1);

			filesPerThread.add(currentFiles);
		}
		filesPerThread.add(allFiles.subList(start, allFiles.size()));

		Thread[] t = new Thread[numProcessors];
		Pipeline pipeline = new Pipeline("", "");
		for (int i = 0; i < numProcessors; i++) {
			TableStatisticsExtractorLocalOnlyEnglish te = new TableStatisticsExtractorLocalOnlyEnglish();
			te.setThreadFiles(filesPerThread.get(i));

			te.setEngFilesTargetFolder(engFilesFolder);
			te.setPipeline(pipeline);
			// te.setDoneWriter(w);
			// te.setQueueManager(new QueueManager(s3AccessKey, s3SecretKey,
			// queueEndpoint, queueName));
			// te.init("", s3Bucket, s3, "tables/", s3OutputBucket,
			// "tablesNonEnglish/");
			t[i] = new Thread(te);
			t[i].start();
		}

		for (int i = 0; i < numProcessors; i++) {
			try {
				t[i].join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		/*
		 * for(String f : files) { try { TableExtractor t = new
		 * TableExtractor(); t.setDoneWriter(w); t.setQueueManager(q); t.init(f,
		 * s3Bucket, s3, targetFolder,s3OutputBucket);
		 * 
		 * pool.execute(t); } catch(Exception e) { e.printStackTrace(); } }
		 * 
		 * pool.shutdown();
		 */
		// w.close();

		/*
		 * System.out.println("Loading file list ..."); boolean isFirst = false;
		 * FileWriter w = new FileWriter("list.txt"); for (String file :
		 * s3.ListBucketContents(s3Bucket, "")) { if (isFirst) {
		 * s3.LoadFileFromS3("file.tar.gz", file, s3Bucket); isFirst = false; }
		 * 
		 * System.out.println(file); w.write(file + "\n"); } w.close();
		 */
		/*
		 * File srcFolder = new File(sourceFolder); for (File arc :
		 * srcFolder.listFiles()) { try { processArcFile(arc); } catch
		 * (Exception e) { // TODO Auto-generated catch block
		 * e.printStackTrace(); } }
		 */
	}

	public void run() {
		try {

			for (String fileName : threadFiles) {
				try {
					processArcFile(fileName);

					// doneWriter.writeLine(fileName);
				} catch (Exception e) {
					e.printStackTrace();
				}

			}

			// doneWriter.writeLine(fileName);
			/*
			 * double p = progressOne(); long time = System.currentTimeMillis()
			 * - startTime; double rest = (1 / p) * time; double seconds = rest
			 * / 1000.0; double minutes = seconds / 60.0; double hours = minutes
			 * / 60.0; double days = hours / 24.0;
			 * 
			 * hours = (days - (int)(days)) * 24; minutes = (hours-
			 * (int)(hours)) * 60;
			 * 
			 * System.out.println(p*100 + " % done - " + (int)days + " days " +
			 * (int)hours + ":" + (int)minutes + "h left.");
			 */
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	protected void processArcFile(String fileName) throws Exception {
		totalFiles = 0;
		processedFiles = 0;

		long start = System.currentTimeMillis();
		// download
		// s3.LoadFileFromS3("/mnt/" + fileName, fileName, s3Bucket);
		long download = System.currentTimeMillis() - start;
		File f = new File(fileName);

		start = System.currentTimeMillis();
		// unzip & untar
		File unzipped = extractTarFile(f.getAbsolutePath());
		if (unzipped == null)
			return;
		// shorter name
		// String soureceFolder = this.sourceFolder;//
		// unzipped.getAbsolutePath().replaceFirst(
		// "common-crawl_parse-output_segment_", "");
		// process
		List<File> filesToCopy = processArcFileContents(unzipped);
		// copy the english files
		String outFolder = engFilesTargetFolder
				+ f.getName().replaceFirst("\\.tar\\.gz$", "");
		File folder = new File(outFolder);
		if (folder.canRead() == false) {
			folder.mkdir();
		}
		List<String> filesToTar = new ArrayList<String>();
		for (File fe : filesToCopy) {
			String fName = copyFileToOutput(fe, outFolder);
			if (fName != null)
				filesToTar.add(fName);
		}
		// tar the files
		String tarfolderStr = tarFilesFromList(outFolder + ".tar", filesToTar);
		// gzip the files
		gzipFile(outFolder + ".tar");
		// remove the tar file
		File tarfolder = new File(outFolder + ".tar");
		tarfolder.delete();
		// remove the original folder
		folder = new File(outFolder);
		for (File f0 : folder.listFiles())
			f0.delete();
		folder.delete();

		// delete all files & folder
		for (File f0 : unzipped.listFiles())
			f0.delete();
		unzipped.delete();
		// f.delete();
		long process = System.currentTimeMillis() - start;

		// upload results
		start = System.currentTimeMillis();

		// upload stats Files
		// s3.SaveFileToS3(gzipedFiles.get(0), "generalStats/"
		// + gzipedFiles.get(0).replaceFirst("/mnt/", ""), s3OutputBucket);
		// s3.SaveFileToS3(gzipedFiles.get(1), "headersStats/"
		// + gzipedFiles.get(1).replaceFirst("/mnt/", ""), s3OutputBucket);
		// s3.SaveFileToS3(gzipedFiles.get(2), "valuesStats/"
		// + gzipedFiles.get(2).replaceFirst("/mnt/", ""), s3OutputBucket);
		//
		// // upload english stats Files
		// s3.SaveFileToS3(gzipedFiles.get(3), "generalStatsEng/"
		// + gzipedFiles.get(3).replaceFirst("/mnt/", ""), s3OutputBucket);
		// s3.SaveFileToS3(gzipedFiles.get(4), "headersStatsEng/"
		// + gzipedFiles.get(4).replaceFirst("/mnt/", ""), s3OutputBucket);
		// s3.SaveFileToS3(gzipedFiles.get(5), "valuesStatsEng/"
		// + gzipedFiles.get(5).replaceFirst("/mnt/", ""), s3OutputBucket);

		double upload = System.currentTimeMillis() - start;

		double up = upload / 1000.0;
		double down = download / 1000.0;
		double proc = process / 1000.0;
		System.out.println("download: " + down + "s; process: " + proc
				+ "s; ratio: " + (down / proc) + "; upload: " + up + "s");

		System.out.println(processedFiles + "/" + totalFiles);
		pipeline.getIndexLogger().info(fileName);
	}

	private List<String> gzipFiles(String path, String prefix) {

		List<String> filesTotar = new LinkedList<String>();
		filesTotar.add(gzipFile(path + prefix + "generalStatistics.txt"));
		filesTotar.add(gzipFile(path + prefix + "headersStatistics.txt"));
		filesTotar.add(gzipFile(path + prefix + "valuesStatistics.txt"));
		filesTotar.add(gzipFile(path + "engStats/" + prefix
				+ "generalStatisticsEng.txt"));
		filesTotar.add(gzipFile(path + "engStats/" + prefix
				+ "headersStatisticsEng.txt"));
		filesTotar.add(gzipFile(path + "engStats/" + prefix
				+ "valuesStatisticsEng.txt"));

		// List<String> filesTotarShort = new LinkedList<String>();

		/*
		 * for (int i = 0; i < filesTotar.size(); i++) {
		 * filesTotarShort.add(filesTotar.get(i).replaceFirst("/mnt/", "")); }
		 */

		// try {
		// tarFilesFromList(path, filesTotar);
		// } catch (Exception e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		return filesTotar;
	}

	private String tarFilesFromList(String absolutePath, List<String> files)
			throws Exception {
		// Output file stream
		FileOutputStream dest = new FileOutputStream(absolutePath);

		// Create a TarOutputStream
		TarOutputStream out = new TarOutputStream(
				new BufferedOutputStream(dest));
		// Files to tar

		for (String fStr : files) {
			File f = new File(fStr);
			out.putNextEntry(new TarEntry(f, f.getName()));
			BufferedInputStream origin = new BufferedInputStream(
					new FileInputStream(f));

			int count;
			byte data[] = new byte[2048];
			while ((count = origin.read(data)) != -1) {
				out.write(data, 0, count);
			}

			out.flush();
			origin.close();
			// f.delete();
		}

		out.close();
		dest.close();
		return null;
	}

	protected List<File> processArcFileContents(File folder) {
		List<List<String>> output = new LinkedList<List<String>>();
		List<String> englishFiles = new ArrayList<String>();
		List<String> nonEglishFiles = new ArrayList<String>();
		File[] jsonFiles = folder.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});

		List<String> filesToCopyToEnglish = new ArrayList<String>();

		for (File json : jsonFiles) {
			final String id = json.getName().split("\\_")[0];

			File[] csvFiles = folder.listFiles(new FilenameFilter() {

				public boolean accept(File dir, String name) {
					if (name.matches(id + "\\_.+\\.csv")
							&& !name.contains("_LINK_"))
						return true;
					return false;
				}
			});
			processTables(json, csvFiles, filesToCopyToEnglish);

		}
		List<File> filesToCopy = new ArrayList<File>();
		for (String str : filesToCopyToEnglish) {
			final String csvPrefix = str.substring(0, str.lastIndexOf("_"));
			final String anyPrefix = str.substring(0, str.indexOf("_"));
			File[] csvFiles = folder.listFiles(new FilenameFilter() {

				public boolean accept(File dir, String name) {
					if (name.matches(csvPrefix + "\\_.+\\.csv")
							|| name.matches(anyPrefix + "\\_.+\\.json")
							|| name.matches(anyPrefix + "\\_\\d+"))
						return true;
					return false;
				}
			});
			for (File f : csvFiles) {
				if (!filesToCopy.contains(f)) {
					filesToCopy.add(f);
				}
			}
		}

		return filesToCopy;

	}

	protected void processTables(File jsonFile, File[] csvFiles,
			List<String> filesToCopyToEnglish) {
		String id = jsonFile.getName().split("\\_")[0];

		// TODO parse json
		List<TableStats> tableStats = new LinkedList<TableStats>();
		try {
			tableStats = getTableStatsFromJson(jsonFile.getAbsolutePath());
		} catch (Exception e) {
			System.out.println(jsonFile.getAbsolutePath());
			e.printStackTrace();
		}
		// for each table
		int tblId = 0;
		for (TableStats stats : tableStats) {

			boolean isEnglish = false;

			// check the size of the table
			if (stats.getLanguage() != null && stats.getLanguage().equals("en")
					&& stats.getNmRows() >= MIN_ROWS
					&& stats.getNmCols() >= MIN_COLS) {
				// check the PLD
				// if it is not English write the table to other folder

				isEnglish = true;
			}
			// use table
			File tableFile = null;

			// find csv file
			for (File csv : csvFiles) {
				if (csv.getName().matches(id + "\\_" + tblId + "+\\_.+")) {
					tableFile = csv;
					if (isEnglish)
						filesToCopyToEnglish.add(tableFile.getName());
					break;
				}
			}

			if (tableFile != null) {
				totalFiles++;
			} else {
				System.out.println("cannot find the file");
			}

			tblId++;
		}

	}

	private void writeLineToFile(String value, Writer writer) {
		// TODO Auto-generated method stub
		try {
			writer.write(value + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public List<TableStats> getTableStatsFromJson(String filePath)
			throws Exception {
		List<TableStats> tableStats = new LinkedList<TableStats>();
		JSONParser parser = new JSONParser();
		// read the file
		JSONObject jsonObject = (JSONObject) parser
				.parse(readJsonStringFromFile(filePath));

		// read the contentTablesArray
		JSONArray tableStatsArray = (JSONArray) jsonObject.get("contentTables");
		{
			for (Object o : tableStatsArray) {
				JSONObject conTable = (JSONObject) o;
				long colNm = (Long) conTable.get("colCount");
				long rowNm = (Long) conTable.get("rowCount");
				String language = "";
				try {
					language = (String) conTable.get("language");
				} catch (Exception e) {

				}
				if (language == null || language.equals("")) {
					String header = (String) jsonObject.get("uri");
					if (isAllowedTld(header))
						language = "en";
				}

				TableStats tStats = new TableStats();
				tStats.setNmCols(colNm);
				tStats.setNmRows(rowNm);
				tStats.setLanguage(language);
				tableStats.add(tStats);

			}
		}
		return tableStats;

	}

	/**
	 * reads json from file and replaces NaN with 0
	 * 
	 * @param file
	 * @return
	 */
	public String readJsonStringFromFile(String file) {
		String jsonString = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			//
			String line = "";
			while ((line = br.readLine()) != null) {
				jsonString += line.replace(":NaN", "0.0");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jsonString;
	}

	protected boolean isAllowedTld(String tld) {
		for (String t : ALLOWED_TLDS)
			if (tld.equals(t))
				return true;
		return false;
	}

	protected String copyFileToOutput(File f, String targetFolderTmp) {
		// return the new file name
		String oFile = null;
		try {
			FileReader r = new FileReader(f);
			String folder = targetFolderTmp;
			if (!targetFolderTmp.endsWith("/"))
				folder += "/";
			oFile = folder + f.getName();
			FileWriter w = new FileWriter(folder + f.getName());

			int i;

			while ((i = r.read()) != -1)
				w.write(i);

			r.close();
			w.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return oFile;
	}

	public static File extractTarFile(String path) {
		try {
			File i = new File(path);

			// this is the extraction folder that needs to be returned
			File outputDir = null;
			String fileName = i.toString();
			String tarFileName = fileName.replaceAll(".gz$", "");
			FileInputStream instream = new FileInputStream(fileName);
			GZIPInputStream ginstream = new GZIPInputStream(instream);
			FileOutputStream outstream = new FileOutputStream(tarFileName);
			byte[] buf = new byte[1024];
			int len;
			while ((len = ginstream.read(buf)) > 0) {
				outstream.write(buf, 0, len);
			}
			ginstream.close();
			outstream.close();
			instream.close();
			// There should now be tar files in the directory
			// extract specific files from tar
			TarArchiveInputStream myTarFile = new TarArchiveInputStream(
					new FileInputStream(tarFileName));
			TarArchiveEntry entry = null;
			int offset;
			FileOutputStream outputFile = null;
			// read every single entry in TAR file
			while ((entry = myTarFile.getNextTarEntry()) != null) {
				// the following two lines remove the .tar.gz extension for
				// the folder name
				fileName = i.getName().substring(0,
						i.getName().lastIndexOf('.'));
				fileName = fileName.substring(0, fileName.lastIndexOf('.'));
				outputDir = new File(i.getParent() + "/" + fileName + "/"
						+ entry.getName());
				if (!outputDir.getParentFile().exists()) {
					outputDir.getParentFile().mkdirs();
				}
				// if the entry in the tar is a directory, it needs to be
				// created, only files can be extracted
				if (entry.isDirectory()) {
					outputDir.mkdirs();
				} else {
					byte[] content = new byte[(int) entry.getSize()];
					offset = 0;
					myTarFile.read(content, offset, content.length - offset);
					outputFile = new FileOutputStream(outputDir);
					IOUtils.write(content, outputFile);
					outputFile.close();
				}
			}
			// close and delete the tar files, and delete the gz file
			myTarFile.close();
			// i.delete();
			File tarFile = new File(tarFileName);
			tarFile.delete();
			outputDir = new File(i.getAbsolutePath().replaceAll(".tar.gz$", ""));
			return outputDir;
		} catch (Exception e) {

			e.printStackTrace();
		}
		return null;
	}

	public String gzipFile(String filePath) {

		byte[] buffer = new byte[1024];
		String outName = filePath + ".gz";

		try {

			FileOutputStream fileOutputStream = new FileOutputStream(outName);

			GZIPOutputStream gzipOuputStream = new GZIPOutputStream(
					fileOutputStream);

			FileInputStream fileInput = new FileInputStream(filePath);

			int bytes_read;

			while ((bytes_read = fileInput.read(buffer)) > 0) {
				gzipOuputStream.write(buffer, 0, bytes_read);
			}

			fileInput.close();

			gzipOuputStream.finish();
			gzipOuputStream.close();
			fileOutputStream.close();
			File file = new File(filePath);
			file.delete();
		} catch (IOException ex) {
			ex.printStackTrace();
			return "";
		}

		return outName;
	}

}
