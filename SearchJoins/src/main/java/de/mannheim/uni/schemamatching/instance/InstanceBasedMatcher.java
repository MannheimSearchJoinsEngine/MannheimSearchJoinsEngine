package de.mannheim.uni.schemamatching.instance;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnMatch;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableMatch;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.TablesLabeledBasedMatcher;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.concurrent.RunnableProgressReporter;

public class InstanceBasedMatcher {

	private HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores;
	
	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> getScores()
	{
		return scores;
	}
	
	public TableObjectsMatch checkForDuplicates(Table t, Pipeline p)
			throws InterruptedException {
		Timer tim = new Timer("Check for duplicates (instance-based)");
		
		calculateScores(t, p);

		TableObjectsMatch result = InstanceBasedComparer.decideObjectMatching(
				scores, t.getHeader(), t.getHeader(), p.getDuplicateLimitInstanceString(), p.getDuplicateLimitInstanceNumeric());

		printTableObjectsMatch(result);

		tim.stop();
		return result;
	}

	public void calculateScores(Table t, Pipeline pipeline) throws InterruptedException
	{
		List<TableColumn> cols = t.getColumns();
		int colCnt = cols.size();
		Timer tCol = new Timer("InstanceBasedColumnComparer for " + colCnt + " columns");
		scores = new HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>>();

		ArrayList<InstanceBasedColumnComparer> comparers = new ArrayList<InstanceBasedColumnComparer>();
		//int numProc = (int) (Runtime.getRuntime().availableProcessors() / 2);
		int numProc = (int) (Runtime.getRuntime().availableProcessors());
		ThreadPoolExecutor pool = new ThreadPoolExecutor(
				numProc, 
				numProc, 
				0,
				TimeUnit.SECONDS,
				new java.util.concurrent.LinkedBlockingQueue<Runnable>());

		System.out.println("Checking " + colCnt + " columns for duplicates:");

		int maxRows=0;
		for (int i = 0; i < cols.size(); i++) {
			TableColumn c1 = cols.get(i);

			if(!c1.isKey() && !c1.getHeader().startsWith("dbpedia_")) // we don't want to fuse the key column (nor the dbpedia reference value)
			{
			
				if(c1.getValues().size()>maxRows)
					maxRows = c1.getValues().size();
				
				scores.put(c1, new HashMap<TableColumn, ColumnScoreValue>());
	
				for (int j = i + 1; j < cols.size(); j++) {
					TableColumn c2 = cols.get(j);
					
                    if(!c2.isKey() && !c1.getHeader().startsWith("dbpedia_")) // we don't want to fuse the key column
                    {
                        //scores.get(c1).put(c2,  new ColumnScoreValue());

                        InstanceBasedColumnComparer cc = new InstanceBasedColumnComparer(pipeline);
                        //t.getColumns().get(0).getValues()
                        cc.setColumn1(c1);
                        cc.setColumn2(c2);

                        comparers.add(cc);

                        if(c1.getDataSource()!=null && c1.getDataSource().equals(c2.getDataSource())
                                        || (c1.getDataType()!=c2.getDataType()))
                        {
                                // do not compare columns from the same table
                                ColumnScoreValue sv = new ColumnScoreValue();
                                sv.Add(0, 0);
                                sv.setType(c1.getDataType().toString());
                                cc.setResults(sv);
                        }
                        else
                                pool.execute(cc);
                    }
				}
			
			}
			else
				System.out.println("skipping key column: " + c1.getHeader());
		}

		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.start();
		
		pool.shutdown();
		
		/*long tasks = pool.getTaskCount();
		long done = pool.getCompletedTaskCount();
		long left = tasks - done;
		while(left>0)
		{
			Thread.sleep(10000);
			tasks = pool.getTaskCount();
			done = pool.getCompletedTaskCount();
			left = tasks - done;
			System.out.println(new Date() + ": " + done + " of " + tasks + " tasks completed.");
		}*/
		
		pool.awaitTermination(1, TimeUnit.DAYS);
		p.stop();
		System.out.println("done.");
		tCol.stop();

		for (InstanceBasedColumnComparer cc : comparers) {
			if(cc.getResults()==null)
			{
				System.out.println("Comparer for columns '" + cc.getColumn1().getHeader() + "' and '" + cc.getColumn2().getHeader() + "' did not run!");
				cc.run();
			}
			
			ColumnScoreValue sv = cc.getResults();
			sv.setTotalCount(maxRows);
			scores.get(cc.getColumn1()).put(cc.getColumn2(), sv);
			scores.get(cc.getColumn2()).put(cc.getColumn1(), sv);
		}
		
		//printMatchingResult(scores);
	}
	
	private void printMatchingResult(HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores)
	{
		try {
			BufferedWriter log = new BufferedWriter(new FileWriter("instancematching.csv"));
			
			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tSimilarity\tComplementarity\tNum Pairs\n");
			
			for (Entry<TableColumn, HashMap<TableColumn, ColumnScoreValue>> en : scores
					.entrySet()) {

				for (Entry<TableColumn, ColumnScoreValue> w : en.getValue()
						.entrySet())
				{
					try
					{       if(w.getValue().getAverage()==0.0) {
                                            continue;
                                        }
						log.write(en.getKey().getHeader().replace('\t', ' ') + "\t");
						log.write(en.getKey().getDataSource() + "\t");
						log.write(en.getKey().getDataType() + "\t");
						log.write(w.getKey().getHeader().replace('\t', ' ') + "\t");
						log.write(w.getKey().getDataSource() + "\t");
						log.write(w.getValue().getAverage() + "\t");
						log.write(w.getValue().getComplementScore() + "\t");
						
						int cnt=0;
						for(Entry<Integer, Double> e : w.getValue().getValues().entrySet())
						{
							if(e.getValue()>0.0)
								cnt++;
						}
						
						log.write(cnt + "");
					}
					catch(Exception e)
					{
						e.printStackTrace();
					}
					
					log.write("\n");
				}
			}
			log.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
	}
	
	private void printTableObjectsMatch(TableObjectsMatch m) {
		try {
			BufferedWriter log = new BufferedWriter(new FileWriter("instancematching_decided.csv"));
			
			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tSimilarity\n");
			
			for (ColumnObjectsMatch c : m.getColumns()) {
				String start = c.getColumn1().getHeader() + "\t" + c.getColumn1().getDataSource() + "\t" + c.getColumnType() + "\t";
				
				
				for (TableColumn tc : c.getColumn2()) {
					log.write(start + tc.getHeader() + "\t" + tc.getDataSource() + "\t" + c.getScore() + "\n");
				}
			}
			
			log.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void printTableMatch(TableMatch m, Integer counter) {
		System.out.println(++counter + ". " + m.getTable1() + " + "
				+ m.getTable2() + " = " + m.getScore());
		for (ColumnMatch c : m.getColumns())
			System.out.println("  " + c.getColumn1() + " + " + c.getColumn2()
					+ " = " + c.getScore() + " (" + c.getColumnType() + ")");

		System.out.println();
	}

	public List<TableMatch> matchResultTables(List<JoinResult> results,
			Pipeline p) throws InterruptedException {
		HashMap<String, List<Entry<IndexEntry, IndexEntry>>> tables = new HashMap<String, List<Entry<IndexEntry, IndexEntry>>>();
		HashMap<String, JoinResult> tableJoins = new HashMap<String, JoinResult>();

		// get all rows from results
		for (JoinResult r : results) {
			tableJoins.put(r.getRightTable(), r);

			if (!tables.containsKey(r.getRightTable()))
				tables.put(r.getRightTable(),
						new ArrayList<Entry<IndexEntry, IndexEntry>>());

			List<Entry<IndexEntry, IndexEntry>> rows = tables.get(r
					.getRightTable());

			for (Entry<IndexEntry, IndexEntry> e : r.getJoinPairs().entrySet()) {
				rows.add(e);
			}
		}

		// determine matching tables
		int tblCnt = tables.size();
		System.out.println("Matching " + tblCnt + " tables.");
		Object[] tbls = tables.values().toArray();
		ArrayList<InstanceBasedResultComparer> comparers = new ArrayList<InstanceBasedResultComparer>();
		ArrayList<TableMatch> matches = new ArrayList<TableMatch>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(
				Runtime.getRuntime().availableProcessors(), 
				Runtime.getRuntime().availableProcessors(), 
				0,
				TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(
						(tblCnt * tblCnt) / 2));
		for (int i = 0; i < tblCnt; i++)
			for (int j = i + 1; j < tblCnt; j++) {
				List<Entry<IndexEntry, IndexEntry>> t1 = (List<Entry<IndexEntry, IndexEntry>>) tbls[i];
				List<Entry<IndexEntry, IndexEntry>> t2 = (List<Entry<IndexEntry, IndexEntry>>) tbls[j];

				// TableComparer tc = new TableComparer();
				InstanceBasedResultComparer tc = new InstanceBasedResultComparer();
				tc.setTable1(t1);
				tc.setTable2(t2);
				tc.setPipeline(p);

				comparers.add(tc);
				pool.execute(tc);

				// TableMatch m = compareTablesFromIndex(t1, t2, p);

				// matches.add(m);
			}

		pool.shutdown();
		pool.awaitTermination(1, TimeUnit.DAYS);

		for (InstanceBasedResultComparer tc : comparers)
			matches.add(tc.getResult());

		// sort matches by score
		Collections.sort(matches, new Comparator<TableMatch>() {

			public int compare(TableMatch o1, TableMatch o2) {
				return -Double.compare(o1.getScore(), o2.getScore());
			}
		});

		// print results
		int i = 0;
		for (TableMatch m : matches) {
			/*
			 * System.out.println(++i + ". " + m.getTable1() + " + " +
			 * m.getTable2() + " = " + m.getScore()); for(ColumnMatch c :
			 * m.getColumns()) System.out.println("  " + c.getColumn1() + " + "
			 * + c.getColumn2() + " = " + c.getScore() + " (" +
			 * c.getColumnType() + ")");
			 * 
			 * System.out.println();
			 */
			printTableMatch(m, i);
		}

		return matches;

		// cluster results
		/*
		 * long start, end; ValueAggregator va = new ValueAggregator();
		 * 
		 * MatchClustering c = new MatchClustering(); start =
		 * System.currentTimeMillis(); List<List<String>> clu =
		 * c.clusterMatchesAgglomerative(matches, 3,
		 * MatchClustering.Linkage.MinDist); end = System.currentTimeMillis();
		 * 
		 * System.out.println("\n\nSingle link (" + ((double)(end-start)/1000.0)
		 * + "s)"); for(int i2=0;i2<clu.size();i2++) { List<String> l =
		 * clu.get(i2); System.out.println("Cluster " + i2); for(String t : l) {
		 * double tableScore = tableJoins.get(t).getRank();
		 * System.out.println("  " + t + "(" + tableScore + ")");
		 * va.AddValue(tableScore); } System.out.println(" Cluster score: min="
		 * + va.getMin() + " max=" + va.getMax() + " avg=" + va.getAvg() +
		 * " sum=" + va.getSum()); va.reset(); }
		 * 
		 * c = new MatchClustering(); start = System.currentTimeMillis(); clu =
		 * c.clusterMatchesAgglomerative(matches, 3,
		 * MatchClustering.Linkage.MaxDist); end = System.currentTimeMillis();
		 * 
		 * System.out.println("\n\nComplete link (" +
		 * ((double)(end-start)/1000.0) + "s)"); for(int
		 * i2=0;i2<clu.size();i2++) { List<String> l = clu.get(i2);
		 * System.out.println("Cluster " + i2); for(String t : l) { double
		 * tableScore = tableJoins.get(t).getRank(); System.out.println("  " + t
		 * + "(" + tableScore + ")"); va.AddValue(tableScore); }
		 * System.out.println(" Cluster score: min=" + va.getMin() + " max=" +
		 * va.getMax() + " avg=" + va.getAvg() + " sum=" + va.getSum());
		 * va.reset(); }
		 * 
		 * c = new MatchClustering(); start = System.currentTimeMillis(); clu =
		 * c.clusterMatchesAgglomerative(matches, 3,
		 * MatchClustering.Linkage.AvgDist); end = System.currentTimeMillis();
		 * 
		 * System.out.println("\n\nAverage link (" +
		 * ((double)(end-start)/1000.0) + "s)"); for(int
		 * i2=0;i2<clu.size();i2++) { List<String> l = clu.get(i2);
		 * System.out.println("Cluster " + i2); for(String t : l) { double
		 * tableScore = tableJoins.get(t).getRank(); System.out.println("  " + t
		 * + "(" + tableScore + ")"); va.AddValue(tableScore); }
		 * System.out.println(" Cluster score: min=" + va.getMin() + " max=" +
		 * va.getMax() + " avg=" + va.getAvg() + " sum=" + va.getSum());
		 * va.reset(); }
		 */
	}
}
