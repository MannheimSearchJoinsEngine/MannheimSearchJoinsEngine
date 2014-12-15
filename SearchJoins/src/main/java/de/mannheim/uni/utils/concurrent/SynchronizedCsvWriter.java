package de.mannheim.uni.utils.concurrent;

import java.io.BufferedWriter;
import java.io.FileWriter;

import au.com.bytecode.opencsv.CSVWriter;

public class SynchronizedCsvWriter
 extends SynchronizedWriter<String[]>
{

	private CSVWriter writer;
	private long length=0;
	
	public SynchronizedCsvWriter(String file) throws Exception {
		super(file);
	}

	@Override
	protected void createWriter(String file) throws Exception {
		writer = new CSVWriter(new BufferedWriter(new FileWriter(file)));
		length=0;
	}

	@Override
	public long getApproximateLength() throws Exception {
		if(writer!=null)
			return length;
		else
			return 0;
	}
	
	@Override
	protected void writeData(String[] data) throws Exception {
		writer.writeNext(data);
		
		for(String s : data)
			length += s.length();
		
		length += (data.length-1); // separator char
	}

	@Override
	protected void flushWriter() throws Exception {
		writer.flush();
	}

	@Override
	protected void closeWriter() throws Exception {
		writer.close();
	}

}
