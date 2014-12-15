package de.mannheim.uni.utils.concurrent;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.Semaphore;

public class SyncFileWriter {

	private BufferedWriter writer;
	
	private Semaphore sem;
	
	public SyncFileWriter(String fileName, boolean append) throws IOException
	{
		writer = new BufferedWriter(new FileWriter(fileName, append));
		sem = new Semaphore(1);
	}
	
	public void writeLine(String line) throws IOException, InterruptedException
	{
		sem.acquire();
		writer.write(line + "\n");
		writer.flush();
		sem.release();
	}
	
	public void close() throws IOException
	{
		writer.close();
	}
	
}
