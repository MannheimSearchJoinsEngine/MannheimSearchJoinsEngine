package de.mannheim.uni.statistics;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.pipelines.Pipeline;

public class DistributionOfTablesPerKey {

	public void analyse(List<JoinResult> result, Pipeline pipe)
	{
		Map<String, Integer> dist = new HashMap<String, Integer>();
		
		for(JoinResult r : result)
		{
			for(IndexEntry e : r.getJoinPairs().keySet())
			{
				int cnt = 0;
				
				if(dist.containsKey(e.getValue()))
					cnt = dist.get(e.getValue());
				
				dist.put(e.getValue(), cnt+1);
			}
		}
		
		DataLogger dl = new DataLogger(pipe);
		dl.logMap(dist, "totalTables");
		dl.logMap(ValueDistribution.generateCount(dist), "totalTablesDist");
	}
	
}
