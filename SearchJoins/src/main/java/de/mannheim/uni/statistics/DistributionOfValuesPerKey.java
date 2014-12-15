package de.mannheim.uni.statistics;

import de.mannheim.uni.IO.TableReader;
import java.util.HashMap;
import java.util.SortedMap;
import java.util.TreeMap;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;

public class DistributionOfValuesPerKey {

	/**
	 * For each key, determine the number of values. 
	 * Generate the distributions, i.e.
	 * key with 2 values -> 20 times
	 * key with 5 values -> 10 times
	 * 
	 * distributions to create:
	 * total number of values per key
	 * number of distinct values per key
	 * number of columns per key (values + nulls) ... should be the same for all keys
	 * 
	 * ...
	 * @param t
	 */
	public void analyse(Table t, Pipeline pipe, TableColumn excludedColumn)
	{
		TreeMap<Integer, Integer> totalValues = new TreeMap<Integer, Integer>();
		TreeMap<Integer, Integer> distinctValues = new TreeMap<Integer, Integer>();
		
		TableColumn key = t.getFirstKey();
		int rows = key.getTotalSize();
		
		// count the number of values for every row
		//for(int i = 1; i < rows; i++)
		for(Integer i : key.getValues().keySet())
		{
			// holds the values for the current row
			HashMap<String, Integer> values = new HashMap<String, Integer>();
			int cnt = 0;			
			
			for(TableColumn c : t.getColumns())
			{
				if(c!=key && c != excludedColumn)
				{
					String value = PipelineConfig.NULL_VALUE;
					
					if(c.getValues().containsKey(i))
						value = c.getValues().get(i);
					
					// if the cell given by c and i has a value
					if(!value.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
					{
						// cnt holds the total number of values for this row
						cnt++;
						
						int valCnt=0;
						
						if(values.containsKey(value))
							valCnt = values.get(value);
						
						// update the count for this specific value
						values.put(value, valCnt+1);
					}
				}
			}
			
			// create the distribution of the values including all counts
			// i.e. add one to the count for keys with cnt values
			int c = 0;
			if(totalValues.containsKey(cnt))
				c = totalValues.get(cnt);
			totalValues.put(cnt, c+1);
			
			// create the distribution of the values including only distinct counts
			// values.size() corresponds to the number of different values in the current row
			c = 0;
			if(distinctValues.containsKey(values.size()))
				c = distinctValues.get(values.size());
			distinctValues.put(values.size(), c+1);
			
		}
		
		DataLogger dl = new DataLogger(pipe);
		dl.logMap(totalValues, "totalValues");
		dl.logMap(distinctValues, "distinctValues");
	}
        
        public static void main(String[] args) {
            DistributionOfValuesPerKey x = new DistributionOfValuesPerKey();
            Pipeline p = new Pipeline(null, null);
            TableReader read = new TableReader();
            Table t = read.readTable(args[0]);
            x.analyse(t, p, null);
        }
	
}
