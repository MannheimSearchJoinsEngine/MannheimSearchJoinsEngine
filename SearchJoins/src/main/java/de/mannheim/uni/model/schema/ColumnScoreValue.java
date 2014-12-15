package de.mannheim.uni.model.schema;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ColumnScoreValue
	implements java.io.Serializable
{
	private Map<Integer,Double> values = new HashMap<Integer,Double>();
	private String type;
	private int totalCount;
	private int complementCount;
	private double sum=0.0;
	private int count=0;
	private int exactMatchCount;
	private boolean watchMe;
	
	public void setWatchMe(boolean watchMe) {
		this.watchMe = watchMe;
	}
	
	
	public int getExactMatchCount() {
		return exactMatchCount;
	}

	public void addExactMatch()
	{
		exactMatchCount++;
	}

	public Map<Integer,Double> getValues()
	{
		return values;
	}
		
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}

	public int getComplementCount() {
		return complementCount;
	}

	public double getSum() {
		return sum;
	}
	public void setSum(double sum) {
		this.sum = sum;
	}
	
	public int getCount() {
		return count;
	}
	public void setCount(int count) {
		this.count = count;
	}

	public int getTotalCount() {
		return totalCount;
	}
	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}
	
	
	public void AddComplement()
	{
		complementCount++;
		// for each complement, a score of 0 is added, so we don't increase the count here
		//count++;
	}
	
	public void Add(double score, int rowId)
	{
		values.put(rowId, score);
		sum += score;
		count++;
		
		if(score==1)
			addExactMatch();
	}
	

	/**
	 * Calculates the average score of all compared pairs
	 * @return sum of score values divided by number of compared pairs
	 */
	public double getAverage()
	{
		if(count==0)
			return 0;
		
		if(watchMe)
		{
			int i =0;
		}
		
		return sum / (double)count;
	}
	
	/**
	 * Calculates the fraction of pairs that are complementary
	 * @return number of complementary pairs divided by total number of compared pairs
	 */
	public double getComplementScore()
	{
		if(count==0)
			return 0;
		
		return (double)complementCount / (double)count;
	}

}
