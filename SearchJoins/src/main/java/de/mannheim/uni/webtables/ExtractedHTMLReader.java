package de.mannheim.uni.webtables;

import de.mannheim.uni.hadoop.TextSequenceFileReader;

public class ExtractedHTMLReader
	extends TextSequenceFileReader
{

	public ExtractedHTMLReader(String inputFile) throws Exception {
		super(inputFile);
	}

	public String getURL()
	{
		return getKey();
	}
	
	public String getHTML()
	{
		return getValue();
	}
	
}
