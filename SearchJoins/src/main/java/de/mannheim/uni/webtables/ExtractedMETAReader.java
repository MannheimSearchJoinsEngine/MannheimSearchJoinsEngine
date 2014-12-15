package de.mannheim.uni.webtables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;

public class ExtractedMETAReader {

	public static List<TableMetaData> readAll(String file) throws IOException
	{
		List<TableMetaData> result = new LinkedList<TableMetaData>();
		CSVReader r = new CSVReader(new FileReader(file));
		
		String[] values = null;
		while((values = r.readNext()) != null)
		{
			result.add(TableMetaData.fromStringArray(values));
		}
		
		return result;
	}
	
	public static Map<String, TableMetaData> readAllMap(String file) throws IOException
	{
		CSVReader r = new CSVReader(new FileReader(file));
		Map<String, TableMetaData> result = new HashMap<String, TableMetaData>();
		
		String[] values = null;
		while((values = r.readNext()) != null)
		{
			TableMetaData data = TableMetaData.fromStringArray(values);
			result.put(data.getUrl(), data);
		}
		
		return result;
	}
}
