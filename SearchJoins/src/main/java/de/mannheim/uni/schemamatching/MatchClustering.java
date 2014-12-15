package de.mannheim.uni.schemamatching;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.mannheim.uni.model.schema.TableMatch;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.statistics.Timer;

public abstract class MatchClustering<T>
{
	private HashMap<String, Integer> cluster;
	private List<T> data;
	private Map<String, T> index;
	private String watch;
	private List<String> watchKeys = new LinkedList<String>();
	
	public MatchClustering()
	{
		cluster = new HashMap<String, Integer>();
		watch = "pixels";
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
	
	public abstract double getNormalizedSimilarity(T object1, T object2);
	
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
		
		for(String key1 : c1)
		{
			T d1 = index.get(key1);
			
			for(String key2 : c2)
			{
				T d2 = index.get(key2);
				
				// score is a similarity measure, so invert to get a distance
				double dist;
				double score = getNormalizedSimilarity(d1, d2);
				
				if(score==0.0)
					dist = Double.MAX_VALUE;
				else
					dist = 1.0 / score;
				
				if(dist<min)
					min = dist;
				if(dist>max)
					max = dist;
				sum+=dist;
				cnt++;
			}
		}
		
		// linkage MinDist: the link with the highest similarity between cluster1 and cluster2 = lowest distance
		// linkage MaxDist: the link with the lowest similarity between cluster1 and cluster2 = highest distance
		// linkage AvgDist: the average similarity between cluster1 and cluster2
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

	public double getIntraClusterSimilarity(List<T> cluster)
	{
		double sum=0;
		int cnt=0;
		
		if(cluster.size()<2)
			return 0;
		
		for(int i=0;i<cluster.size();i++)
			for(int j=i+1;j<cluster.size();j++)
			{
				sum += getNormalizedSimilarity(cluster.get(i), cluster.get(j));
				cnt++;
			}
		
		return sum/(double)cnt;
	}
	
	private void mergeCluster(int cluster1, int cluster2)
	{
		boolean log=false;
		
		for(String s : watchKeys)
			if(cluster.get(s)==cluster1 || cluster.get(s)==cluster2)
			{
				log=true;
				break;
			}
		
		if(log)
		{
			System.out.println("Merging clusters " + cluster1 + " and " + cluster2);
			System.out.println("Cluster " + cluster1 + ": ");

			for(Entry<String, Integer> e : cluster.entrySet())
			{
				if(e.getValue()==cluster1)
					System.out.println("\t" + e.getKey());
			}
			
			System.out.println("Cluster " + cluster2 + ": ");
		}
		
		for(Entry<String, Integer> e : cluster.entrySet())
		{
			if(e.getValue()==cluster2)
			{
				if(log)
					System.out.println("\t" + e.getKey());
				e.setValue(cluster1);
			}
		}
	}
	
	public void printClusters()
	{
		List<Integer> clu = getClusters();
		
		Collections.sort(clu);
		
		for(int i : clu)
		{
			for(Entry<String, Integer> e : cluster.entrySet())
				if(e.getValue()==i)
					System.out.println("Cluster " + i + ": " + e.getKey());
		}
	}
	
	public abstract String getKey(T value);
	
	public abstract boolean canMerge(MatchCluster<T> cluster1, MatchCluster<T> cluster2, double similarity);
	
	public List<MatchCluster<T>> clusterMatchesAgglomerative(List<T> data, Linkage l) throws Exception
	{
		Timer tim = new Timer("Run Clustering");
		index = new HashMap<String, T>();
		this.data = data;
		
		// initialize clustering
		int clusterId=0;
		for(int i=0;i<data.size();i++)
		{
			String key = getKey(data.get(i));
			if(!cluster.containsKey(key))
					cluster.put(key, clusterId++);
			index.put(key, data.get(i));
			if(key.contains(watch))
				watchKeys.add(key);
		}
		
		// loop until desired number of clusters is created
		List<Integer> clu = getClusters();
		int lastSize=-1;
		long lastMsg = System.currentTimeMillis();
		double[][] distances = new double[clu.size()][clu.size()];
		int[] minDist = new int[clu.size()];
		try
		{
			HashMap<Integer, List<Integer>> mergeClusters = new HashMap<Integer, List<Integer>>();
			
			while(clu.size()!=lastSize)
			{
				//System.out.println("Clustering ... " + clu.size() + " clusters");
				//printClusters();
				
				int i_min=-1, j_min=-1;
				double dist_min=Double.MAX_VALUE;
				// merge the two closest clusters
				for(int i=0;i<clu.size();i++)
				{
					List<Integer> merge = new LinkedList<Integer>();
					
					for(int j=i+1;j<clu.size();j++)
					{
						double dist = getDistance(clu.get(i), clu.get(j), l);
						
						distances[i][j] = dist;
						distances[j][i] = dist;
						
						if(dist<dist_min)
						{
							dist_min = dist;
							minDist[i] = j;
							i_min=clu.get(i);
							j_min=clu.get(j);
						}
					}
	
					// only merge the current cluster, if it fulfills the requirements ...
					if(!canMerge(getCluster(i_min, l), getCluster(j_min, l), 1/dist_min))
						continue;
	
					merge.add(j_min);
					
					// also merge all other clusters with the same distance that are not closer to any other cluster
					for(int j=i+1;j<clu.size();j++)
					{
						if(distances[i][j]<=dist_min)
						{
							boolean add=true;
							// check that no other cluster is closer
							for(int k=0;k<clu.size();k++)
							{
								if(k!=j)
								{
									if(distances[j][k]<distances[i][j])
										add=false;
									break;
								}
							}
							
							if(add && !merge.contains(clu.get(j)))
								merge.add(clu.get(j));
						}
					}
					
					mergeClusters.put(i_min, merge);
				}
				
				for(Entry<Integer, List<Integer>> merges : mergeClusters.entrySet())
					for(int c : merges.getValue())
						mergeCluster(merges.getKey(), c);		
				mergeClusters.clear();
				
				
				//System.out.println("Merging " + i_min + " and " + j_min);
				//mergeCluster(i_min, j_min);
				
				lastSize = clu.size();
				
				clu = getClusters();
				
				
				if(System.currentTimeMillis()-lastMsg>10000)
				{
					System.out.println(new java.util.Date() + ": " + clu.size() + " clusters left, current score = " + 1/dist_min);
					lastMsg = System.currentTimeMillis();
				}
			}
		}
		catch(Exception e)
		{
			printClusters();
			throw e;
		}
		
		List<MatchCluster<T>> results = new ArrayList<MatchCluster<T>>();
		for(int c : clu)
		{
			/*List<T> cluLst = new ArrayList<T>();
			
			for(Entry<String, Integer> e : cluster.entrySet())
				if(e.getValue()==c)
					cluLst.add(index.get(e.getKey()));*/
			
			//results.add(cluLst);
			results.add(getCluster(c, l));
			
		}
		
		tim.stop();
		return results;
	}	
	
	public MatchCluster<T> getCluster(int c, Linkage l)
	{
		List<T> cluLst = new ArrayList<T>();
		
		for(Entry<String, Integer> e : cluster.entrySet())
			if(e.getValue()==c)
				cluLst.add(index.get(e.getKey()));
		
		MatchCluster<T> clu = new MatchCluster<T>();
		clu.setCluster(cluLst);
		clu.setDistance(getDistance(c, c, l));
		
		return clu;
	}
}