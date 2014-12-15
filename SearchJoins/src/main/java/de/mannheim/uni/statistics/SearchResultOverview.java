package de.mannheim.uni.statistics;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;

import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.pipelines.Pipeline;

public class SearchResultOverview {

	public void generate(String[] paths, Pipeline p, String output) throws IOException
	{
		String[] header = new String[] { "property", "#jointables", "#foundkeys" };
		String[] values = new String[header.length];
		
		CSVWriter w = new CSVWriter(new FileWriter(output));
		w.writeNext(header);
		
		for(String path : paths)
		{
			File f = new File(path, "results.queryTableCoverage.single.csv");
			List<JoinResult> results = JoinResult.readCsv(f.getAbsolutePath());
			
			values[0] = path;
			values[1] = Integer.toString(countTables(results));
			values[2] = Integer.toString(countKeys(results).size());
			w.writeNext(values);
		}
		
		w.close();
		
	}
	
	private int countTables(List<JoinResult> results)
	{
		TreeSet<String> tbls = new TreeSet<String>();
		
		for(JoinResult r : results)
		{
			for(IndexEntry e : r.getJoinPairs().values())
			{
				tbls.add(e.getFullTablePath());
			}
		}
		
		return tbls.size();
	}
	
	private Map<String, Integer> countKeys(List<JoinResult> results)
	{
		
		Map<String, Integer> keys = new HashMap<String, Integer>();
		for(JoinResult r : results)
		{
			for(IndexEntry e : r.getJoinPairs().keySet())
			{
				int cnt = 0;
				
				if(keys.containsKey(e.getValue()))
					cnt = keys.get(e.getValue());
				
				keys.put(e.getValue(), cnt+1);
			}
		}
		
		return keys;
	}
}
