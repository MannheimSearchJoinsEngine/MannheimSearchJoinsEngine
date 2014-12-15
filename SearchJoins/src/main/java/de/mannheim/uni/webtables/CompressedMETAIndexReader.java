package de.mannheim.uni.webtables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mannheim.uni.model.Pair;

public class CompressedMETAIndexReader {

	public static List<Pair<String, Integer>> readMapping(String file) throws NumberFormatException, IOException
	{
		BufferedReader r = new BufferedReader(new FileReader(file));
		List<Pair<String, Integer>> result = new LinkedList<Pair<String, Integer>>();
		
		String line = null;
		
		while((line = r.readLine()) != null)
		{
			String[] values = line.split("\t");
			
			if(values.length>1)
				result.add(new Pair<String, Integer>(values[0], Integer.parseInt(values[1])));
		}
		
		return result;
	}
	
	public static Map<String, Integer> readMappingAsMap(String file) throws NumberFormatException, IOException
	{
		BufferedReader r = new BufferedReader(new FileReader(file));
		Map<String, Integer> result = new HashMap<String, Integer>();
		
		String line = null;
		
		while((line = r.readLine()) != null)
		{
			String[] values = line.split("\t");
			
			if(values.length>1)
				result.put(values[0], Integer.parseInt(values[1]));
		}
		
		return result;
	}
	
	public static Map<Integer, String> readMappingInvertedAsMap(String file) throws NumberFormatException, IOException
	{
		BufferedReader r = new BufferedReader(new FileReader(file));
		Map<Integer, String> result = new HashMap<Integer, String>();
		
		String line = null;
		
		while((line = r.readLine()) != null)
		{
			String[] values = line.split("\t");
			
			if(values.length>1)
				result.put(Integer.parseInt(values[1]),values[0]);
		}
		
		return result;
	}
}
