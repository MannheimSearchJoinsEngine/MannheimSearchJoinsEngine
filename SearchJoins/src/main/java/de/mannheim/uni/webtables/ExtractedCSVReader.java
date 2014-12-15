package de.mannheim.uni.webtables;

import de.mannheim.uni.hadoop.TextSequenceFileReader;

public class ExtractedCSVReader 
	extends TextSequenceFileReader
{

	public ExtractedCSVReader(String inputFile) throws Exception {
		super(inputFile);
	}

	public String getFileName()
	{
		return getKey();
	}
	
	public String getCSV()
	{
		return getValue();
	}
	
}
