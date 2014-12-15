package de.mannheim.uni.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;

import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.schemamatching.label.StringNormalizer;

public class GoldStandardUpdater {

	public static void main(String[] args) throws IOException
	{
		if(args.length!=3)
		{
			System.out.println("Parameters:");
			System.out.println("1. file name");
			System.out.println("2. old version");
			System.out.println("3. new version");
			return;
		}
		
		int v1=-1, v2=-1;
		
		v1 = Integer.parseInt(args[1]);
		v2 = Integer.parseInt(args[2]);
		
		GoldStandardUpdater upd = new GoldStandardUpdater();
		
		upd.update(args[0], v1, v2);
	}
	
	public void update(String file, int oldVersion, int newVersion) throws IOException
	{
		int currentVersion = oldVersion;
		
		List<JoinResult> results = JoinResult.readCsv(file);
		JoinResult.writeCsv(results, file + ".bak");
		
		while(currentVersion<newVersion)
		{
			if(currentVersion==1)
			{
				for(JoinResult r : results)
				{
					for(Entry<IndexEntry, IndexEntry> entry : r.getJoinPairs().entrySet())
					{
						IndexEntry e;
						
						e = entry.getKey();
						e.setValue(StringNormalizer.clearString(e.getValue(),true));
						
						e = entry.getValue();
						e.setValue(StringNormalizer.clearString(e.getValue(),true));
					}
				}
				
				currentVersion++;
			}
		}
		
		JoinResult.writeCsv(results, file);
	}
}
