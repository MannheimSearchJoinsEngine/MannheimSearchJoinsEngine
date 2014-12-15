package de.mannheim.uni.schemamatching.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import de.mannheim.uni.model.schema.TableMatch;

public class MatchClustering
{
	private HashMap<String, Integer> cluster;
	private List<TableMatch> matches;
	
	public MatchClustering()
	{
		cluster = new HashMap<String, Integer>();
	}
	
	private List<Integer> getClusters()
	{
		List<Integer> list = new ArrayList<Integer>();
		
		for(int i : cluster.values())
			if(!list.contains(i))
				list.add(i);
		
		return list;
	}
	
	public enum Linkage
	{
		MinDist,
		MaxDist,
		AvgDist
	}
	
	private double getDistance(int cluster1, int cluster2, Linkage l)
	{
		List<String> c1, c2;
		c1 = new ArrayList<String>();
		c2 = new ArrayList<String>();
		
		for(Entry<String, Integer> e : cluster.entrySet())
		{
			if(e.getValue()==cluster1)
				c1.add(e.getKey());
			else if(e.getValue()==cluster2)
				c2.add(e.getKey());
		}
		
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		double sum = 0;
		int cnt = 0;
		
		// linkage MinDist: the match with the highest similarity between cluster1 and cluster2
		// linkage MaxDist: the match with the lowest similarity between cluster1 and cluster2
		// linkage AvgDist: the average similarity between cluster1 and cluster2
		
		for(TableMatch m : matches)
		{
			if(c1.contains(m.getTable1()) && c2.contains(m.getTable2())
					|| c1.contains(m.getTable2()) && c2.contains(m.getTable1()))
			{
				// score is a similarity measure, so invert to get a distance
				double dist;
				double score = m.getScore();
				
				if(score==0.0)
					dist = Double.MAX_VALUE;
				else
					dist = 1.0 / m.getScore();
				
				if(m.getScore()<min)
					min = dist;
				if(m.getScore()>max)
					max = dist;
				sum+=dist;
				cnt++;
			}
		}
		
		switch(l)
		{
		case MinDist:
			return min;
		case MaxDist:
			return max;
		case AvgDist:
			return sum / (double)cnt;
		default:
			return 0;
		}
	}

	private void mergeCluster(int cluster1, int cluster2)
	{
		for(Entry<String, Integer> e : cluster.entrySet())
			if(e.getValue()==cluster2)
				e.setValue(cluster1);
	}
	
	public void printClusters()
	{
		List<Integer> clu = getClusters();
		
		for(int i : clu)
		{
			for(Entry<String, Integer> e : cluster.entrySet())
				if(e.getValue()==i)
					System.out.println("Cluster " + i + ": " + e.getKey());
		}
	}
	
	public List<List<String>> clusterMatchesAgglomerative(List<TableMatch> matches, int numClusters, Linkage l)
	{
		List<List<String>> results = new ArrayList<List<String>>();
		this.matches = matches;
		
		// initialize clustering
		int clusterId=0;
		for(int i=0;i<matches.size();i++)
		{
			if(!cluster.containsKey(matches.get(i).getTable1()))
					cluster.put(matches.get(i).getTable1(), clusterId++);
		}
		
		// loop until desired number of clusters is created
		List<Integer> clu = getClusters();
		int lastSize=-1;
		while(clu.size()>numClusters)
		{
			//System.out.println("Clustering ... " + clu.size() + " clusters");
			//printClusters();
			
			int i_min=-1, j_min=-1;
			double dist_min=Double.MAX_VALUE;
			// merge the two closest clusters
			for(int i=0;i<clu.size();i++)
			{
				for(int j=i+1;j<clu.size();j++)
				{
					double dist = getDistance(clu.get(i), clu.get(j), l);
					
					if(dist<dist_min)
					{
						dist_min = dist;
						i_min=clu.get(i);
						j_min=clu.get(j);
					}
				}
			}
			
			//System.out.println("Merging " + i_min + " and " + j_min);
			mergeCluster(i_min, j_min);
			
			lastSize = clu.size();
			
			clu = getClusters();
			
			if(clu.size()==lastSize)
				break;
		}
		
		for(int c : clu)
		{
			List<String> cluLst = new ArrayList<String>();
			
			for(Entry<String, Integer> e : cluster.entrySet())
				if(e.getValue()==c)
					cluLst.add(e.getKey());
			
			results.add(cluLst);
			
		}
		
		return results;
	}	
}