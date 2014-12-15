package de.mannheim.uni.statistics;

import java.util.HashMap;
import java.util.Map;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;

public class ComplementarityAnalyser {

	public void analyse(Table t, TableColumn targetColumn, Pipeline pipe)
	{
		TableColumn key = t.getFirstKey();
		
		HashMap<Integer, Integer> complementary = new HashMap<Integer, Integer>();
		HashMap<Integer, Integer> evidence = new HashMap<Integer, Integer>();
		
		for(Integer row : key.getValues().keySet())
		{
			boolean isEmpty = isEmpty(targetColumn, row);
			int valCnt = 0;
			
			for(TableColumn c : t.getColumns())
			{
				if(!isEmpty(c, row) && !c.isKey() && c!=targetColumn)
					valCnt++;
			}
			
			if(isEmpty)
				complementary.put(row, valCnt);
			else
				evidence.put(row, valCnt);
		}
		
		DataLogger log = new DataLogger(pipe);
		log.logMap(complementary, "complementary");
		log.logMap(evidence, "evidence");
		Map<Integer, Integer> cdist = ValueDistribution.generateCount(complementary);
		log.logMap(cdist, "complementaryDist");
		Map<Integer, Integer> edist = ValueDistribution.generateCount(evidence);
		log.logMap(edist, "evidenceDist");
		
		Map<Integer, Integer> cdistSum = new HashMap<Integer, Integer>();
		if(cdist.containsKey(0))
			cdistSum.put(0, cdist.get(0));
		else
			cdistSum.put(0, 0);
		int sum=0;
		for(Integer k : cdist.keySet())
			if(k!=0)
				sum += cdist.get(k);
		cdistSum.put(1,  sum);
		log.logMap(cdistSum, "complementarySummed");
		
		Map<Integer, Integer> edistSum = new HashMap<Integer, Integer>();
		if(edist.containsKey(0))
			edistSum.put(0, edist.get(0));
		else
			edistSum.put(0, 0);
		sum=0;
		for(Integer k : edist.keySet())
			if(k!=0)
				sum += edist.get(k);
		edistSum.put(1,  sum);
		log.logMap(edistSum, "evidenceSummed");
	}
	
	private boolean isEmpty(TableColumn c, int row)
	{
		return !c.getValues().containsKey(row) 
				|| c.getValues().get(row) == null 
				|| c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE);
	}
	
}
