package de.mannheim.uni.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.model.*;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.scoring.ScoreEvaluator;

public class SearchTableResultAnalyzer {

	public SearchTableResultAnalyzer()
	{
		
	}
	
	private class ValueGroup
	{
		private double min;
		private double max;
		private int count;
		private double sum;
		
		public ValueGroup()
		{
			min = Double.MAX_VALUE;
			max = Double.MIN_VALUE;
			count = 0;
		}
		
		public void AddValue(double d)
		{
			sum += d;
			count++;
			
			if(d>max)
				max = d;
			if(d<min)
				min = d;
		}
		
		public double getAverage()
		{
			return sum / (double)count;
		}
		
		public double getMax()
		{
			return max;
		}
		
		public double getMin()
		{
			return min;
		}
		
		public double getSum()
		{
			return sum;
		}
		
		@Override
		public String toString() {
			return "min: " + getMin() + "; "
					+ "max: " + getMax() + "; "
					+ "avg: " + getAverage();
		}
	}
	
	public class SearchTableResultStatistics
	{
		private String entityTableName;
		
		private int correctMatches;
		
		private int totalMatches;
		
		private int totalInstances;
		
		private double rankingScore;

		public double getRankingScore() {
			return rankingScore;
		}

		public void setRankingScore(double rankingScore) {
			this.rankingScore = rankingScore;
		}

		public int getTotalInstances() {
			return totalInstances;
		}

		public void setTotalInstances(int totalInstances) {
			this.totalInstances = totalInstances;
		}

		public String getEntityTableName() {
			return entityTableName;
		}

		public void setEntityTableName(String entityTableName) {
			this.entityTableName = entityTableName;
		}

		public int getCorrectMatches() {
			return correctMatches;
		}

		public void setCorrectMatches(int correctMatches) {
			this.correctMatches = correctMatches;
		}

		public int getTotalMatches() {
			return totalMatches;
		}

		public void setTotalMatches(int totalMatches) {
			this.totalMatches = totalMatches;
		}
		
		public double getPrecision()
		{
			return (double)correctMatches / (double)totalMatches;
		}
		
		public double getCoverage()
		{
			return (double)totalMatches / (double)totalInstances;
		}
		
		@Override
		public String toString() {
			double precision = getPrecision();
			double coverage = getCoverage();  
			
			DecimalFormatSymbols decimalFormatSymbols = new DecimalFormatSymbols();
			decimalFormatSymbols.setDecimalSeparator('.');
			decimalFormatSymbols.setGroupingSeparator(',');
			DecimalFormat decimalFormat = new DecimalFormat("0.000", decimalFormatSymbols);
			
			return correctMatches 
					+ "\t" + totalMatches 
					+ "\t" + totalInstances 
					+ "\t" + decimalFormat.format(precision) 
					+ "\t" + decimalFormat.format(coverage) 
					+ "\t" + entityTableName
					+ "\t" + rankingScore;
		}
	}
	
	private JoinResult mergeTableResults(Map<String, JoinResult> goldStandard)
	{
		if(goldStandard.size()==0)
			return null;
		
		// merges the values from all tables in the gold standard
		JoinResult result = new JoinResult();
		
		JoinResult first = goldStandard.entrySet().iterator().next().getValue();
		
		result.setLeftColumn(first.getLeftColumn());
		result.setLeftColumnCardinality(first.getLeftColumnCardinality());
		
		// iterate over all JoinResults
		for(Entry<String, JoinResult> e : goldStandard.entrySet())
		{
			// Consider all matched values
			for(Entry<IndexEntry, IndexEntry> i : e.getValue().getJoinPairs().entrySet())
			{
				// Don't add duplicate values
				// Determine all values for the current key 
				List<Entry<IndexEntry,IndexEntry>> test = getEntries(result.getJoinPairs(), i.getKey().getValue());
				
				// iterate over these values to see whether the new value already exists
				boolean exists=false;
				for(Entry<IndexEntry, IndexEntry> t : test)
				{
					if(t.getValue().getValue().equals(i.getValue().getValue()))
					{
						// the value already exists
						exists=true;
						break;
					}
				}
				
				if(!exists)
				{
					// add value (i.getKey is a new instance of IndexEntry, hence the same string key value can be added multiple times)
					result.getJoinPairs().put(i.getKey(), i.getValue());
				}
			}
		}
		
		return result;
	}
	
	private List<String> readIncorrectTables(String file)
	{
		ArrayList<String> results = new ArrayList<String>();
		
		try
		{
			BufferedReader reader = new BufferedReader(new FileReader(file));
			
			String line=null;
			
			while((line = reader.readLine()) != null)
				results.add(line);
			
			reader.close();
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return results;
	}
	
	public List<EvaluatedJoinResult> analyzeResult(Table queryTable, List<JoinResult> result, Map<String, JoinResult> goldStandard, String queryFile, Pipeline p, double duration, String incorrectTablesFile)
	{
		StringBuilder results = new StringBuilder();
		StringBuilder details = new StringBuilder();
		StringBuilder entityCoverage = new StringBuilder();
		List<String> wrongTables = readIncorrectTables(incorrectTablesFile);
		List<EvaluatedJoinResult> evaluation = new ArrayList<EvaluatedJoinResult>();
		
		List<SearchTableResultStatistics> stats = new ArrayList<SearchTableResultStatistics>();
		SearchTableResultStatistics stat;
		
		ValueGroup ttlPrecision = new ValueGroup();
		ValueGroup ttlCoverage = new ValueGroup();
		
		HashSet<String> keys = new HashSet<String>();
		int numKeysInQuery=0;
		
		JoinResult mergedGoldStandard = mergeTableResults(goldStandard);
		
		HashMap<String, HashMap<Integer, Boolean>> queryEntityCoverage = new HashMap<String, HashMap<Integer,Boolean>>();
		
		entityCoverage.append("Table Index\tEntity Key\tKey Value Match\n");
		for(TableColumn c : queryTable.getColumns())
		{
			if(c.isKey())
			{
				for(String key : c.getValues().values())
				{
					queryEntityCoverage.put(key, new HashMap<Integer, Boolean>());
					
					for(int i=0; i<result.size();i++)
					{
						queryEntityCoverage.get(key).put(i, null);
					}
				}
			}
		}
		
		int resultIndex = 0;
		for(JoinResult r : result)
		{
			resultIndex++;
			EvaluatedJoinResult er = new EvaluatedJoinResult();
			er.setResult(r);
			evaluation.add(er);
			// collect result statistics
			stat = new SearchTableResultStatistics();
			stat.setEntityTableName(r.getRightTable());
			stat.setTotalMatches(r.getCount());
			stat.setTotalInstances(r.getLeftColumnCardinality());
			stat.setRankingScore(r.getTotalRank());
			stats.add(stat);
			
			numKeysInQuery = r.getLeftColumnCardinality();
			
			//details.append("Details for entity table " + r.getRightTable() + "\n");
			
			// Determine values from gold standard
			//JoinResult gold = goldStandard.get(r.getRightTable());
			JoinResult gold = mergedGoldStandard;
			
			if(wrongTables.contains(r.getRightTable()))
			{
				stat.setCorrectMatches(0);
				details.append(r.getRightTable() + ": Table does not match at all!\n");
			}
			else if(gold==null)
			{
				// no gold standard available
				details.append(r.getRightTable() + ": No gold standard available!\n");
			}
			else
			{
				// compare actual values to gold standard
				
				// Key column
				//details.append("Key column is " + r.getRightColumn() + " ");
				/*if(r.getRightColumn().equals(gold.getRightColumn()))
					;//details.append("(correct)\n");
				else
					details.append(r.getRightTable() + ": Key column is " + r.getRightColumn() + " (wrong, should be " + gold.getRightColumn() + ")\n");
				*/
				
				// key value matching
				for(Entry<IndexEntry, IndexEntry> row : r.getJoinPairs().entrySet())
				{
					er.setCorrect(row.getValue(), false);
					
					List<Entry<IndexEntry, IndexEntry>> goldEntries = getEntries(gold.getJoinPairs(), row.getKey().getValue());
					
					if(goldEntries == null || goldEntries.size()==0)
					{
						// Value not contained in gold standard (should not happen)
						// assume that the gold standard is incomplete
						details.append(r.getRightTable() + ": Key missing in gold standard: " + row.getKey().getValue() + "\n");
					}
					else
					{
						boolean correctMatch=false;
						// check all entries 
						for(Entry<IndexEntry, IndexEntry> g : goldEntries)
						{
							// Correct match
							if(row.getValue().getValue().equalsIgnoreCase(g.getValue().getValue()))
							{
								// increment counter
								stat.setCorrectMatches(stat.getCorrectMatches()+1);
								
								// add correctly matched key to the list of matched keys
								if(!keys.contains(row.getKey().getValue()))
									keys.add(row.getKey().getValue());
								
								correctMatch=true;
								er.setCorrect(row.getValue(), true);
								break;
							}
						} //for(Entry<IndexEntry, IndexEntry> g : goldEntries)
						// Incorrect match
						
						if(!correctMatch)
						{
							//details.append(r.getRightTable() + ": Incorrect match (score " + row.getValue().getLuceneScore() + "): " + row.getKey().getValue() + " [");
							double score = ScoreEvaluator.get(p).assessIndexEntry(row.getValue());
							details.append(r.getRightTable() + ": Incorrect match (score " + score + "): " + row.getKey().getValue() + " [");
							
							for(int i=0;i<row.getKey().getRefineAttrs().size();i++)
								details.append((i==0?"":", ") + row.getKey().getRefineAttrs().get(i).getValue());
							
							details.append("] --> " + row.getValue().getValue() + " [");
							
							for(int i=0;i<row.getValue().getRefineAttrs().size();i++)
							{
								IndexEntry entry = row.getValue().getRefineAttrs().get(i);
								double entryScore = ScoreEvaluator.get(p).assessIndexEntry(entry);
								details.append((i==0?"":", ") + entry.getValue() + " (" + entryScore + ")");
							}
							
							details.append("]\n");
							
							queryEntityCoverage.get(row.getKey().getValue()).put(resultIndex, false);
						}
						else
							queryEntityCoverage.get(row.getKey().getValue()).put(resultIndex, true);
					}
				} // for(Entry<IndexEntry, IndexEntry> row : r.getJoinPairs().entrySet())
			} // if(gold==null) else-part
			
			results.append(stat.toString() + "\n");
			ttlPrecision.AddValue(stat.getPrecision());
			ttlCoverage.AddValue(stat.getCoverage());
			
			// evaluate entity coverage statistics
			for(String key : queryEntityCoverage.keySet())
			{
				Boolean b = queryEntityCoverage.get(key).get(resultIndex);
				String value = b == null ? "missing" : (b ? "correct" : "incorrect");
				entityCoverage.append(resultIndex + "\t" + key + "\t" + value + "\n");
			}
			
		} // for(JoinResult r : result)
		
		
		System.out.println("\n\nResult Statistics\n\n");
		
		// report overall statistics
		System.out.println("Overall PRECISION: ");
		System.out.println(ttlPrecision.toString());
		
		System.out.println("Overall COVERAGE:");
		System.out.println(ttlCoverage.toString());
		
		System.out.println("Total number of values from query table matched: " + keys.size());
		double combinedPrecision = ((double)keys.size() / (double)numKeysInQuery);
		System.out.println("Combined PRECISION of all results: " + combinedPrecision);
		
		// report JoinResult statistics
		System.out.println("\nEntity Table Statistics\n");
		System.out.println("#tp\t#tp+fp\t#tp+fn\tprec\tcov\ttable\trank");
		System.out.print(results.toString());
		try
		{
			BufferedWriter w = new BufferedWriter(new FileWriter("search_statistics.csv"));
			w.write("#tp\t#tp+fp\t#tp+fn\tprec\tcov\ttable\trank\n");
			w.write(results.toString());
			w.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		// report details
		System.out.println("\nAnalysis Details\n");
		System.out.print(details.toString());
		
		// report entity coverage
		System.out.println("\nEntity Coverage Statistics");
		System.out.println(entityCoverage.toString());
		try
		{
			BufferedWriter w = new BufferedWriter(new FileWriter("entity_coverage.csv"));
			w.write(entityCoverage.toString());
			w.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		writeStatistics(queryFile, p, ttlPrecision, ttlCoverage, combinedPrecision, duration);
		writeResults(queryFile, results.toString(), p);
		
		try {
			EvaluatedJoinResult.writeDetailedCsv(evaluation, "../details.csv", true);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return evaluation;
	}
/*
	public void analyzeResultSimple(List<JoinResult> result, Map<String, String[]> validKeys)
	{
		// unused
		List<SearchTableResultStatistics> stats = new ArrayList<SearchTableResultStatistics>();
		ValueGroup ttlPrecision = new ValueGroup();
		ValueGroup ttlCoverage = new ValueGroup();
		HashSet<String> keys = new HashSet<String>();
		int numKeysInQuery=0;
		StringBuilder results = new StringBuilder();
		results.append("#tp\t#tp+fp\t#tp+fn\tprec\tcov\ttable\trank\n");
		for(JoinResult r : result)
		{
			numKeysInQuery = r.getLeftColumnCardinality();
			
			SearchTableResultStatistics stat = new SearchTableResultStatistics();
			
			stat.setEntityTableName(r.getRightTable());
			stat.setTotalMatches(r.getCount());
			stat.setTotalInstances(r.getLeftColumnCardinality());
			stats.add(stat);
			
			for(Entry<IndexEntry, IndexEntry> e : r.getJoinPairs().entrySet())
			{
				String[] correct = validKeys.get(e.getKey().getValue());
				
				for(String c : correct)
					if(c.equals(e.getValue().getValue()))
					{
						stat.setCorrectMatches(stat.getCorrectMatches()+1);
						if(!keys.contains(e.getKey().getValue()))
							keys.add(e.getKey().getValue());
					}
			}
			
			results.append(stat.toString() + "\n");
			ttlPrecision.AddValue(stat.getPrecision());
			ttlCoverage.AddValue(stat.getCoverage());
		}
		
		System.out.println("\n\nResult Statistics\n");
		
		// report overall statistics
		System.out.println("Overall PRECISION: ");
		System.out.println(ttlPrecision.toString());
		
		System.out.println("Overall COVERAGE:");
		System.out.println(ttlCoverage.toString());
		
		System.out.println("Total number of values from query table matched: " + keys.size());
		System.out.println("Combined COVERAGE of all results: " + ((double)keys.size() / (double)numKeysInQuery) + "\n");
		
		System.out.println(results.toString());
	}
*/
	private void writeStatistics(String queryFile, Pipeline p, ValueGroup precision, ValueGroup coverage, double combinedCoverage, double duration)
	{
		try{
			//File q = new File(queryFile);
			
			//String fileName = new File(q.getName(), "stats.csv").getPath();
			String fileName = "../stats.csv";
			
			CSVWriter writer = new CSVWriter(new FileWriter(fileName, true));

			String[] values = new String[13];
			//query	
			values[0] = queryFile;
			
			//index
			values[1] = p.getIndexLocation();
			
			//key identification
			values[2] = p.getKeyidentificationType().toString();

			// ranking type
			values[3] = p.getRankingType().toString();
			
			//precision
			//-min	
			values[4] = Double.toString(precision.getMin());
			//-max
			values[5] = Double.toString(precision.getMax());
			//-avg	
			values[6] = Double.toString(precision.getAverage());
			
			//coverage
			//-min
			values[7] = Double.toString(coverage.getMin());
			//-max	
			values[8] = Double.toString(coverage.getMax());
			//-avg
			values[9] = Double.toString(coverage.getAverage());
			//-combined
			values[10] = Double.toString(combinedCoverage);
			
			//duration
			values[11] = Double.toString(duration);
			
			//configuration
			values[12] = p.getConfigFileLocation();
			
			writer.writeNext(values);
			
			writer.close();	
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	private void writeResults(String queryFile, String results, Pipeline p)
	{
		try{
			//File q = new File(queryFile);
			
			//String fileName = new File(q.getParent(), "stats.csv").getPath();
			String fileName = "../results.csv";
			
			CSVWriter writer = new CSVWriter(new FileWriter(fileName, true));

			String[] lines = results.split("\n");
			
			int order=1;
			
			for(String line : lines)
				if(!line.isEmpty())
					writer.writeNext(
							(
									queryFile 
									+ "\t" + p.getIndexLocation()
									+ "\t" + p.getKeyidentificationType().toString()
									+ "\t" + p.getRankingType().toString()
									+ "\t" + order++ 
									+ "\t" + line
									+ "\t" + p.getConfigFileLocation()
							).split("\t"));

			writer.close();	
		}
		catch(Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public Map<String, String[]> readValidKeys(String filename) throws IOException
	{
		BufferedReader reader = new BufferedReader(new FileReader(filename));
		Map<String, String[]> result = new HashMap<String, String[]>();
		String line=null;
		String[] values=null;
		
		while((line = reader.readLine())!=null)
		{
			values = line.split(ConvertFileToTable.delimiter);
			
			String[] v = new String[values.length-1];
			
			for(int i=1;i<values.length;i++)
				v[i-1] = values[i];
			
			result.put(values[0], v);
		}
		
		reader.close();
		
		return result;
	}
	
	public static Entry<IndexEntry, IndexEntry> getEntry(Map<IndexEntry, IndexEntry> map, String queryValue)
	{
		for(Entry<IndexEntry, IndexEntry> row : map.entrySet())
		{
			if(row.getKey().getValue().equals(queryValue))
				return row;
		}
		
		return null;
	}
	
	private List<Entry<IndexEntry, IndexEntry>> getEntries(Map<IndexEntry, IndexEntry> map, String queryValue)
	{
		List<Entry<IndexEntry, IndexEntry>> lst = new ArrayList<Map.Entry<IndexEntry,IndexEntry>>();
		
		for(Entry<IndexEntry, IndexEntry> row : map.entrySet())
		{
			if(row.getKey().getValue().equals(queryValue))
				lst.add(row);
		}
		
		return lst;
	}
}
