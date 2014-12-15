package de.mannheim.uni.webtables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.hadoop.SynchronizedSequenceFileWriter;
import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.utils.ByteCount;
import de.mannheim.uni.utils.DomainUtils;
import de.mannheim.uni.utils.concurrent.SynchronizedCsvWriter;

public class TableDataWriter {

	private SynchronizedSequenceFileWriter<FileSet> htmlWriter;
	private SynchronizedSequenceFileWriter<TableStats> csvWriter;
	private SynchronizedCsvWriter metaWriter;
	private String currentFileName;
	private String prefix;
	
	private String currentHtmlFile, currentCsvFile, currentMetaFile;
	private FileUploader htmlUploader, csvUploader, metaUploader;
	
	public TableDataWriter(String prefix, FileUploader htmlUploader, FileUploader csvUploader, FileUploader metaUploader) throws Exception
	{
		this.prefix = prefix;
		createNextPart();
		
		this.htmlUploader = htmlUploader;
		this.csvUploader = csvUploader;
		this.metaUploader = metaUploader;
	}
	
	public void nextPart() throws Exception
	{
		uploadCurrentPart();
		createNextPart();
	}
	
	protected void createNextPart() throws Exception
	{
		currentFileName = prefix + "_" + UUID.randomUUID().toString();

		createHtmlWriter();
		createCsvWriter();
		createMetaWriter();
	}
	
	public void uploadCurrentPart()
	{
		final String html = currentHtmlFile;
		final String csv = currentCsvFile;
		final String meta = currentMetaFile;
		final SynchronizedSequenceFileWriter<FileSet> htmlWriter = this.htmlWriter;
		final SynchronizedSequenceFileWriter<TableStats> csvWriter = this.csvWriter;
		final SynchronizedCsvWriter metaWriter = this.metaWriter;
		
		Thread t = new Thread(new Runnable() {
			
			public void run() {
				try {
					htmlWriter.flushAndBlock();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					csvWriter.flushAndBlock();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				try {
					metaWriter.flushAndBlock();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				htmlUploader.upload(html);
				csvUploader.upload(csv);
				metaUploader.upload(meta);
			}
		});
		
		t.start();
	}
	
	protected void createHtmlWriter() throws Exception
	{
		currentHtmlFile = currentFileName + "_HTML";
		htmlWriter = new SynchronizedSequenceFileWriter<FileSet>(currentHtmlFile) {
			
			@Override
			protected String getValue(FileSet data) {
				try {
					return FileUtils.readFileToString(data.getHtmlFile());
				} catch (IOException e) {
					e.printStackTrace();
					return "";
				}
			}
			
			@Override
			protected String getKey(FileSet data) {
				return data.getURL();
			}
		};
	}
	
	protected void createCsvWriter() throws Exception
	{
		currentCsvFile = currentFileName + "_CSV";
		csvWriter = new SynchronizedSequenceFileWriter<TableStats>(currentCsvFile) {
			
			@Override
			protected String getValue(TableStats data) {
				try {
					return FileUtils.readFileToString(new File(data.getHeader()));
				} catch (IOException e) {
					e.printStackTrace();
					return "";
				}
			}
			
			@Override
			protected String getKey(TableStats data) {
				return new File(data.getHeader()).getName();
			}
		};
	}
	
	protected void createMetaWriter() throws Exception
	{
		currentMetaFile = currentFileName + "_META";
		metaWriter = new SynchronizedCsvWriter(currentMetaFile);
	}
	
	public void writeTableData(FileSet fs) throws InterruptedException
	{
		// write data to current file
		
		// HTML page
		htmlWriter.write(fs);
		
		for(WebTableStats stat : fs.getCsvStats())
		{
			// CSV tables
			csvWriter.write(stat);
			
			// metadata/statistics
			String[] values = new String[7];
			values[0] = fs.getURL();
			values[1] = DomainUtils.getTopLevelDomainFromWholeURL(fs.getURL());
			values[2] = new File(stat.getHeader()).getName();
			values[3] = Integer.toString((int)stat.getNmCols());
			values[4] = Integer.toString((int)stat.getNmRows());
			values[5] = Long.toString(stat.getTableStart());
			values[6] = Long.toString(stat.getTableEnd());
			metaWriter.write(values);
		}
	}
	
}
