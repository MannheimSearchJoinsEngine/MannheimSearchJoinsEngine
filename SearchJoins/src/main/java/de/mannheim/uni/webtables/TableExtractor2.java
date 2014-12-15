package de.mannheim.uni.webtables;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.DomainUtils;
import de.mannheim.uni.utils.TarArchive;
import de.mannheim.uni.utils.aws.LocalQueueManager;
import de.mannheim.uni.utils.aws.QueueManager;
import de.mannheim.uni.utils.aws.S3Helper;
import de.mannheim.uni.utils.concurrent.SynchronizedTextWriter;

public class TableExtractor2
{

	public static final int MIN_COLS = 3;
	public static final int MIN_ROWS = 5;
	public static final String[] ALLOWED_TLDS = new String[] { "com", "net",
			"org", "uk", "eu" };
	
	private QueueManager qm;
	private S3Helper s3;
	private FileDownloader sourceManager;
	private String sourceBucket;
	private String destinationBucket;
	private TableDataWriter englishWriter;
	private TableDataWriter nonEnglishWriter;
	private SynchronizedTextWriter doneWriter;
	private int numThreads = Runtime.getRuntime().availableProcessors();
	
	public void setNumThreads(int numThreads) {
		this.numThreads = numThreads;
	}
	
	public List<File> tempFolders;
	
	public String getSourceBucket() {
		return sourceBucket;
	}
	
	public String getDestinationBucket() {
		return destinationBucket;
	}
	
	public TableExtractor2(String awsKey, String awsSecret, String queueEndpoint, String queueName, String sourceBucket, String destinationBucket) throws Exception
	{
		qm = new QueueManager(awsKey, awsSecret, queueEndpoint, queueName);
		s3 = new S3Helper(awsKey, awsSecret);
		doneWriter = new SynchronizedTextWriter("processed_files.txt");
		this.sourceBucket = sourceBucket;
		this.destinationBucket = destinationBucket;
		
		FileUploader fakeUploader = new FileUploader() {
			
			@Override
			public void upload(String file) {
				System.out.println("TODO: upload " + file);
			}
		};
		
		sourceManager = new FileDownloader() {
			
			@Override
			public String download(String source, String target) {
				String key = source.replace("s3://", "");
				key = key.replace(getSourceBucket(), "");
				if(key.startsWith("/"))
					key = key.substring(1);
				
				s3.LoadFileFromS3(key, key, getSourceBucket());
				return key;
			}
		};
		
		englishWriter = new TableDataWriter("english", fakeUploader, fakeUploader, fakeUploader);
		nonEnglishWriter = new TableDataWriter("nonEnglish", fakeUploader, fakeUploader, fakeUploader);
	}
	
	public static void main(String[] args) {
		//runLocal();
		//runWithLocalQueue();
		
		Integer threads = null;
		
		
		String mode = args[0];
		
		if(args.length>1)
			threads = Integer.parseInt(args[1]);
		
		if(mode.equals("aws"))
			runAWS(threads);
		else if(mode.equals("localqueue"))
			runWithLocalQueue();
		else if(mode.equals("local"))
			runLocal();
	}

	public static void runAWS(Integer threads)
	{
		try {
			TableExtractor2 te = new TableExtractor2("AKIAJUYAKM5IVKM2RJHA", "tnBh/fRgjGHeb5lUz4fzaHbKnY6T9AWkbyT281nG", "sqs.us-east-1.amazonaws.com", "SearchJoinsQueue", "WebTablesExtraction", "dummy");
			
			if(threads!=null)
				te.setNumThreads(threads);
			
			te.startExtraction();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void runWithLocalQueue()
	{
		List<String> queueItems = new LinkedList<String>();
		
		try {
			String files = FileUtils.readFileToString(new File("files.txt"));
			List lines = FileUtils.readLines(new File("files.txt"));
			for(Object f : lines)
				queueItems.add((String)f);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		LocalQueueManager queue = new LocalQueueManager(queueItems);
		
		try {
			TableExtractor2 te = new TableExtractor2(queue, "AKIAJUYAKM5IVKM2RJHA", "tnBh/fRgjGHeb5lUz4fzaHbKnY6T9AWkbyT281nG", "WebTablesExtraction");
			
			te.startExtraction();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void runLocal()
	{
		List<String> queueItems = new LinkedList<String>();
		
		/*try {
			File f = new File("/data/htmlTablesTarCompleted/");
			
			for(String fi : f.list())
				queueItems.add("/data/htmlTablesTarCompleted/" + fi);
			
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		try {
			String files = FileUtils.readFileToString(new File("files.txt"));
			List lines = FileUtils.readLines(new File("files.txt"));
			for(Object f : lines)
				queueItems.add((String)f);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		
		LocalQueueManager queue = new LocalQueueManager(queueItems);
		
		try {
			TableExtractor2 te = new TableExtractor2(queue);
			
			te.startExtraction();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public TableExtractor2(LocalQueueManager queue, String awsKey, String awsSecret, String sourceBucket) throws Exception
	{
		qm = queue;
		s3 = new S3Helper(awsKey, awsSecret);
		doneWriter = new SynchronizedTextWriter("processed_files.txt");
		this.sourceBucket = sourceBucket;
		this.destinationBucket = null;
		
		FileUploader fakeUploader = new FileUploader() {
			
			@Override
			public void upload(String file) {
				System.out.println("upload " + file);
			}
		};
		
		sourceManager = new FileDownloader() {
			
			@Override
			public String download(String source, String target) {
				s3.LoadFileFromS3(target, source, getSourceBucket());
				return target;
			}
		};
		
		englishWriter = new TableDataWriter("english", fakeUploader, fakeUploader, fakeUploader);
		nonEnglishWriter = new TableDataWriter("nonEnglish", fakeUploader, fakeUploader, fakeUploader);
	}
	
	public TableExtractor2(LocalQueueManager queue) throws Exception
	{
		qm = queue;
		doneWriter = new SynchronizedTextWriter("processed_files.txt");
		
		FileUploader fakeUploader = new FileUploader() {
			
			@Override
			public void upload(String file) {
				System.out.println("TODO upload " + file);
			}
		};
		
		sourceManager = new FileDownloader() {
			
			@Override
			public String download(String source, String target) {
				try {
					File f = new File(new File(target).getName());
					FileUtils.copyFile(new File(source), f);
					return f.getName();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return source;
			}
		};
		
		englishWriter = new TableDataWriter("english", fakeUploader, fakeUploader, fakeUploader);
		nonEnglishWriter = new TableDataWriter("nonEnglish", fakeUploader, fakeUploader, fakeUploader);
	}
	
	public void startExtraction()
	{
		int numProcessors = this.numThreads;

		tempFolders = new LinkedList<File>();
		
		Thread[] t = new Thread[numProcessors];

		for (int i = 0; i < numProcessors; i++) {
			TableExtractor2Worker worker = new TableExtractor2Worker();
			
			t[i] = new Thread(worker);
			t[i].start();
		}

		for (int i = 0; i < numProcessors; i++) {
			try {
				t[i].join();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		englishWriter.uploadCurrentPart();
		nonEnglishWriter.uploadCurrentPart();
		try {
			doneWriter.stopAndBlock();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		for(File f : tempFolders)
			try {
				FileUtils.deleteDirectory(f);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	}

	public class TableExtractor2Worker implements Runnable
	{

		public void run() {
			try {			
				String fileName = qm.nextFile();

				while (fileName != null && !fileName.isEmpty()) {
					try {
						processArcFile(fileName);
						qm.setFileProcessed();
						doneWriter.write(fileName);
					} catch (Exception e) {
						e.printStackTrace();
					}

					fileName = qm.nextFile();
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	

	
	protected void processArcFile(String fileName) throws Exception {
		System.out.println("processing " + fileName);
		Timer tim = Timer.getNamed("processAcrFile", null);
		
		// download
		Timer tDown = Timer.getNamed("Download file", tim);
		//s3.LoadFileFromS3(fileName, fileName, getSourceBucket());
		fileName = sourceManager.download(fileName, fileName);
		tDown.stop();

		// unzip & untar
		Timer tExt = Timer.getNamed("Extract archive", tim);
		File f = new File(fileName);
		//TarArchive tar = new TarArchive(f.getAbsolutePath());
		TarArchive tar = new TarArchive(fileName);
		File tarArchive = tar.extract();
		tExt.stop();
		
		if(tar.getExtracted()==null)
			System.out.println("Unzipped of " + f.getAbsolutePath() + " is null!");
		else
		{
			// get different kinds of files: json, html, csv
			Timer tFilter = Timer.getNamed("Filter tables", tim);
			List<FileSet> files = FileSet.getFileSets(tar.getExtracted());
			
			List<FileSet> englishTables = new LinkedList<FileSet>();
			List<FileSet> nonEnglishTables = new LinkedList<FileSet>();
			
			// filter tables based on json
			for(FileSet fs : files)
			{
				String tld = DomainUtils.getTopLevelDomainFromWholeURL(fs.getURL());
				boolean isEnglishTable = isAllowedTld(tld);
				
				List<TableStats> remove = new LinkedList<TableStats>();
				
				for(WebTableStats stat : fs.getCsvStats())
				{
					if (stat.getNmRows() >= MIN_ROWS && stat.getNmCols() >= MIN_COLS) {
						// we keep this table
					}
					else
					{
						// we don't want this table
						remove.add(stat);
					}
				}
				
				fs.RemoveTables(remove);
				
				// only keep if there is at least one table left
				if(fs.getCsvFiles().size()>0)
				{
					if(isEnglishTable)
					{
						// add to english output
						englishTables.add(fs);
					}
					else
					{
						// add to non-english output
						nonEnglishTables.add(fs);
					}
				}
			}
			tFilter.stop();
			
			// write remaining tables to ...
			Timer tWrite = Timer.getNamed("Write data", tim);
			for(FileSet fs : englishTables)
				englishWriter.writeTableData(fs);
			for(FileSet fs : nonEnglishTables)
				nonEnglishWriter.writeTableData(fs);
			tWrite.stop();
			
			//IMPORTANT!!!
			// don't delete the extracted tar here, the data may still be in the writer's queue!
			tempFolders.add(tarArchive);
		}
		tim.stop();
	}
	
	protected boolean isAllowedTld(String tld) {
		if(tld==null)
			return false;
		
		for (String t : ALLOWED_TLDS)
			if (tld.equals(t))
				return true;
		return false;
	}
}
