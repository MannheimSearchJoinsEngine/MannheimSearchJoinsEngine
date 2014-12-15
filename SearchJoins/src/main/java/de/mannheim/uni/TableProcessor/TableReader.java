package de.mannheim.uni.TableProcessor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.model.IndexEntry;

public class TableReader {

	private String filename;
	private BufferedReader reader = null;
	private int currentLine = 0;
	private boolean isSilent=false;
	private String currentLineContents=null;
	
	public boolean getIsSilent()
	{
		return isSilent;
	}
	
	public void setIsSilent(boolean value)
	{
		isSilent = value;
	}
	
	public int getCurrentLine()
	{
		return currentLine;
	}
	
	public TableReader(String tableFileName)
	{
		filename = tableFileName;
	}
	
	public void open() throws UnsupportedEncodingException, FileNotFoundException, IOException
	{
		// Open table for reading
		reader = new BufferedReader(
				new InputStreamReader(
						new GZIPInputStream(
								new FileInputStream(filename)),"UTF-8"));
		
		currentLine = 0;
	}
	
	public void close() throws IOException
	{
		if(reader!=null)
			reader.close();
	}
	
	public String[] GetEntryValues(int entryId) throws IOException
	{					
		if(currentLine==entryId)
		{
			System.out.println("Reading line " + entryId + " again ...");
			return currentLineContents.split(ConvertFileToTable.delimiter);
		}
		
		if(!isSilent)
			System.out.println("Searching line " + entryId + " ...");
		
		// Move to correct line		
		while((currentLineContents = reader.readLine()) != null)
		{
			currentLine++;
			
			if(currentLine==entryId)
				return currentLineContents.split(ConvertFileToTable.delimiter);
		}
		
		// end of file reached
		if(!isSilent)
			System.out.println("Reached end of file at line " + currentLine);
		
		return null;
	}
}
