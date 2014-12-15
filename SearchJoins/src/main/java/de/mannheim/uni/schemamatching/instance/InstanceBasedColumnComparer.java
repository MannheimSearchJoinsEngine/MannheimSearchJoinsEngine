package de.mannheim.uni.schemamatching.instance;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;

import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;

public class InstanceBasedColumnComparer extends InstanceBasedComparer {
	private TableColumn column1;
	private TableColumn column2;
	private ColumnScoreValue results;
	private boolean useDeviation = false;
	private Pipeline pipeline;
	
	public InstanceBasedColumnComparer(Pipeline p)
	{
		pipeline = p;
	}
	
	public boolean isUseDeviation() {
		return useDeviation;
	}

	public void setUseDeviation(boolean useDeviation) {
		this.useDeviation = useDeviation;
	}

	public ColumnScoreValue getResults() {
		return results;
	}

	protected void setResults(ColumnScoreValue results) {
		this.results = results;
	}

	public TableColumn getColumn1() {
		return column1;
	}

	public void setColumn1(TableColumn column1) {
		this.column1 = column1;
	}

	public TableColumn getColumn2() {
		return column2;
	}

	public void setColumn2(TableColumn column2) {
		this.column2 = column2;
	}
	
	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}
	
	public Pipeline getPipeline() {
		return pipeline;
	}

	public void run() {
		ColumnScoreValue result = compareColumns(column1, column2, useDeviation);
		setResults(result);
	}

	public ColumnScoreValue compareColumns(TableColumn c1, TableColumn c2) {
		return compareColumns(c1, c2, false);
	}

	public ColumnScoreValue compareColumns(TableColumn c1, TableColumn c2,
			boolean useDeviation) {
		ColumnScoreValue sv = new ColumnScoreValue();

		/*if(
				(c1.getHeader().equals("dbpedia_almaMater") || c2.getHeader().equals("dbpedia_almaMater"))
				&&
				(c1.getHeader().startsWith("2 unlimited") || c2.getHeader().startsWith("2 unlimited"))
		)
		{
			sv.setWatchMe(true);
		}*/
		
		// sv.setTotalCount(Math.max(c1.getTotalSize(), c2.getTotalSize()));

		sv.setType(c2.getDataType().toString());
		
		if(c1.getDataType()==ColumnDataType.string && c1.getDataType()==ColumnDataType.string && false)
		{
			// !!! the blocking does not work this way !!!
			
			// use blocking approach for string values
			sv.Add(compareAllValues(c1, c2), 0);
		}
		else
		{
			Map<Integer, String> values1, values2;

			values1 = c1.getValues();
			values2 = c2.getValues();

			// merge the ids of the rows that contain a value for at least one
			// column
			HashSet<Integer> rows = new HashSet<Integer>();
			HashSet<Integer> pairs = new HashSet<Integer>();
			for (Integer rowId : values1.keySet())
				rows.add(rowId);
			for (Integer rowId : values2.keySet())
				if(rows.contains(rowId))
					pairs.add(rowId);
				else
					rows.add(rowId);

			// get the range of values for both columns
			MinMax mm = null;

			System.out.println(pairs.size() + " pairs to check");
			
			if (c1.getDataType() == c2.getDataType() && rows.size() > 0) {

				// Min/Max values are only needed for 'date'-type columns
				if(c1.getDataType()==ColumnDataType.date)
					mm = getMinMaxValues(c1.getValues(), c2.getValues(), c1.getDataType());
			} else
				//mm = new MinMax();
				return sv;
			
			//TODO skip comparisons if pairs.size() is too low to reach threshold taking into account rows.size()
			
			if(pipeline.getInstanceMatchingSampleRatio()<1.0)
			{
				// Create a random sample of all rows that contain values
				int ttl = pairs.size();
				LinkedList<Integer> rowList = new LinkedList<Integer>(pairs);
				HashSet<Integer> sample = new HashSet<Integer>();
				double sampleRatio = 0.3;
				int sampleSize = (int) (ttl*sampleRatio);
				Random r = new Random();
				
				while(sample.size()<sampleSize)
					sample.add(rowList.remove(r.nextInt(rowList.size())));
				
				rows = sample;
				//System.out.println("Sample size is " + sample.size());
			}
			
			// iterate over all rows
			for (Integer rowId : rows) {
				double score = 0.0;
	
				String v1 = null, v2 = null;
				
				boolean v1null=false, v2null=false;
				
				if(values1.containsKey(rowId))
					v1 = values1.get(rowId);
				else
					v1null = true;
				
				if(values2.containsKey(rowId))
					v2 = values2.get(rowId);
				else
					v2null = true;				
	
				// we can only compare if there is a value for both rows
				if (v1null && v2null)
					continue; // both values null, so we do not calculate a
								// score for this value combination
				else if (!v1null && !v2null) {
					// none of the values is null, we can calculate a score
					if (!useDeviation)
						score = compareColumnValues(
								c1.getDataType().toString(), v1,
								c1.getHeader(), c2.getDataType().toString(),
								v2, c2.getHeader(), mm);
					else
						score = getValueDeviation(c1.getDataType().toString(),
								v1, c1.getHeader(),
								c2.getDataType().toString(), v2,
								c2.getHeader(), mm);
				} else
					// if only one of the values is null, the score is 0
					sv.AddComplement();
					
				sv.Add(score, rowId);
	
				if (sv.getAverage() > 1.0) {
					System.out.println("Normalization error: " + c1.getHeader()
							+ "(" + v1 + ")" + " * " + c2.getHeader() + "(" + v2
							+ ")" + "[" + c1.getDataType().toString() + "] = "
							+ score);
					System.out.print("\t" + sv.getCount() + " values: ");
					for (Double d : sv.getValues().values())
						System.out.print(d + " ");
					System.out.println();
				}
			}
		}

		return sv;
	}
}
