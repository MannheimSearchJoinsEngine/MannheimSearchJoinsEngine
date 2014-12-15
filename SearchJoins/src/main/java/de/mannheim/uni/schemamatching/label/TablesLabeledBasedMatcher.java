package de.mannheim.uni.schemamatching.label;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.lod.SPARQLEndpointQueryRunner;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.instance.InstanceBasedComparer;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.concurrent.RunnableProgressReporter;

/**
 * @author petar
 * 
 */
public class TablesLabeledBasedMatcher {
	private LabelBasedComparer comparer;

	/**
	 * Used for matching table headers in the ranking process
	 * 
	 * @param tables
	 */
	public void matchTables(Collection<JoinResult> tables) {
		if (tables.size() == 0)
			return;
		ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 0,
				TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(
						tables.size()));

		for (JoinResult jr : tables) {
			TableLabeledBasedMatcherThread tc = new TableLabeledBasedMatcherThread(
					jr);

			pool.execute(tc);
		}
		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.start();

		pool.shutdown();
		try {
			pool.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		p.stop();
	}

	private HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores;

	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> getScores() {
		return scores;
	}

	/**
	 * Use for calculating columns labels similarity
	 * 
	 * @param t
	 * @return
	 * @throws InterruptedException
	 */
	public TableObjectsMatch checkForDuplicates(Table t, Pipeline p)
			throws InterruptedException {
		Timer tim = new Timer("Check for duplicates (label-based)");

		calculateScores(t);

		System.out.println("Deciding label-based matching ...");
		long start = System.currentTimeMillis();
		TableObjectsMatch result = InstanceBasedComparer.decideObjectMatching(
				scores, t.getHeader(), t.getHeader(),
				p.getDuplicateLimitInstanecLabelString(),
				p.getDuplicateLimitInstanecLabelNumeric());

		p.getLogger().info(
				"Deciding label-based matching took "
						+ (System.currentTimeMillis() - start) / 1000 + "s");

		printTableObjectsMatch(result, new Integer(1));

		tim.stop();
		return result;
	}

	public TableObjectsMatch checkForDuplicates(Table t, TableObjectsMatch instancematches, Pipeline p)
			throws InterruptedException {
		Timer tim = new Timer("Check for duplicates (label-based)");

		calculateScores(instancematches);

		System.out.println("Deciding label-based matching ...");
		long start = System.currentTimeMillis();
		
		TableObjectsMatch result = InstanceBasedComparer.decideObjectMatching(
				scores, t.getHeader(), t.getHeader(),
				p.getDuplicateLimitInstanecLabelString(),
				p.getDuplicateLimitInstanecLabelNumeric());

		p.getLogger().info(
				"Deciding label-based matching took "
						+ (System.currentTimeMillis() - start) / 1000 + "s");

		printTableObjectsMatch(result, new Integer(1));

		tim.stop();
		return result;
	}
	
	private boolean shouldRunMatcher(ColumnObjectsMatch instanceMatch)
	{
		switch (instanceMatch.getColumn1().getDataType()) {
		case string:
		case link:
		case bool:
			if (instanceMatch.getScore() >= pipeline
					.getDuplicateLimitInstanceString()) {
				return false;
			}
			// check if there is a label match
			if (instanceMatch.getScore() >= pipeline
							.getDuplicateLimitInstanecLabelString()) {
				return true;

			}

			break;
		case numeric:
		case unit:
		case date:
		case coordinate:
			if (instanceMatch.getScore() >= pipeline
					.getDuplicateLimitInstanceNumeric()) {
				return false;
			}
			if (instanceMatch.getScore() >= pipeline
							.getDuplicateLimitInstanecLabelNumeric()) {
				return true;
			}

			break;
		default:
			
		}
		
		return false;
	}
	
	public void calculateScores(TableObjectsMatch instancematches) throws InterruptedException
	{
		//List<TableColumn> cols = t.getColumns();
		//int colCnt = cols.size();
		Timer tCol = new Timer("LabelBasedColumnComparer for " + instancematches.getColumns().size()
				+ " instance matches");
		scores = new HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>>();

		ArrayList<LabelBasedColumnComparer> comparers = new ArrayList<LabelBasedColumnComparer>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors() / 2, Runtime.getRuntime()
				.availableProcessors() / 2, 60, TimeUnit.SECONDS,
				new java.util.concurrent.LinkedBlockingQueue<Runnable>());

		for(ColumnObjectsMatch match : instancematches.getColumns())
		{
			TableColumn c1 = match.getColumn1();
			
			if (!scores.containsKey(c1))
				scores.put(c1, new HashMap<TableColumn, ColumnScoreValue>());
			
			if(shouldRunMatcher(match))
			{
				for(TableColumn c2 : match.getColumn2())
				{
					LabelBasedColumnComparer cc = new LabelBasedColumnComparer(
							pipeline, queryTable);
					cc.setColumn1(c1);
					cc.setColumn2(c2);
	
					comparers.add(cc);
	                if(c1.getDataSource()!=null && c1.getDataSource().equals(c2.getDataSource())
	                        || (c1.getDataType()!=c2.getDataType())) 
	                {
						// do not compare columns from the same table or with different type
						ColumnScoreValue sv = new ColumnScoreValue();
						sv.Add(0, 0);
						cc.setResults(sv);
					} else
						pool.execute(cc);
				}
			}
		}

		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.start();

		pool.shutdown();
		pool.awaitTermination(1, TimeUnit.DAYS);
		p.stop();
		System.out.println("done.");
		tCol.stop();

		for (LabelBasedColumnComparer cc : comparers) {
			scores.get(cc.getColumn1()).put(cc.getColumn2(), cc.getResults());
			scores.get(cc.getColumn2()).put(cc.getColumn1(), cc.getResults());
		}

		//printMatchingResult(scores);
	}
	
	public void calculateScores(Table t) throws InterruptedException {
		List<TableColumn> cols = t.getColumns();
		int colCnt = cols.size();
		Timer tCol = new Timer("LabelBasedColumnComparer for " + colCnt
				+ " columns");
		scores = new HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>>();

		ArrayList<LabelBasedColumnComparer> comparers = new ArrayList<LabelBasedColumnComparer>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors(), Runtime.getRuntime()
				.availableProcessors(), 60, TimeUnit.SECONDS,
				new java.util.concurrent.LinkedBlockingQueue<Runnable>());

		for (int i = 0; i < cols.size(); i++) {
			TableColumn c1 = cols.get(i);

			if (!scores.containsKey(c1))
				scores.put(c1, new HashMap<TableColumn, ColumnScoreValue>());

			for (int j = i + 1; j < cols.size(); j++) {
				TableColumn c2 = cols.get(j);

				LabelBasedColumnComparer cc = new LabelBasedColumnComparer(
						pipeline, queryTable);
				cc.setColumn1(c1);
				cc.setColumn2(c2);

				comparers.add(cc);
				//if (c1.getDataSource() != null && c1.getDataSource().equals(c2.getDataSource())) {
                if(c1.getDataSource()!=null && c1.getDataSource().equals(c2.getDataSource())
                        || (c1.getDataType()!=c2.getDataType())) 
                {
					// do not compare columns from the same table or with different type
					ColumnScoreValue sv = new ColumnScoreValue();
					sv.Add(0, 0);
					cc.setResults(sv);
				} else
					pool.execute(cc);
			}
		}

		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.start();

		pool.shutdown();
		pool.awaitTermination(1, TimeUnit.DAYS);
		p.stop();
		System.out.println("done.");
		tCol.stop();

		for (LabelBasedColumnComparer cc : comparers) {
			scores.get(cc.getColumn1()).put(cc.getColumn2(), cc.getResults());
			scores.get(cc.getColumn2()).put(cc.getColumn1(), cc.getResults());
		}

		//printMatchingResult(scores);
	}

	private void printMatchingResult(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores) {
		try {
			BufferedWriter log = new BufferedWriter(new FileWriter(
					"labelmatching.csv"));

			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tSimilarity\n");

			for (Entry<TableColumn, HashMap<TableColumn, ColumnScoreValue>> en : scores
					.entrySet()) {

				for (Entry<TableColumn, ColumnScoreValue> w : en.getValue()
						.entrySet()) {
					try {
						log.write(en.getKey().getHeader() + "\t");
						log.write(en.getKey().getDataSource() + "\t");
						log.write(en.getKey().getDataType() + "\t");
						log.write(w.getKey().getHeader() + "\t");
						log.write(w.getKey().getDataSource() + "\t");
						log.write(w.getValue().getAverage() + " ");
					} catch (Exception e) {
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

	public static void printTableObjectsMatch(TableObjectsMatch m,
			Integer counter) {

		try {
			BufferedWriter log = new BufferedWriter(new FileWriter(
					"labelmatching_decided.csv"));

			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tSimilarity\n");

			for (ColumnObjectsMatch c : m.getColumns()) {
				String start = c.getColumn1().getHeader() + "\t"
						+ c.getColumn1().getDataSource() + "\t"
						+ c.getColumnType() + "\t";

				for (TableColumn tc : c.getColumn2()) {
					log.write(start + tc.getHeader() + "\t"
							+ tc.getDataSource() + "\t" + c.getScore() + "\n");
				}
			}

			log.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private Pipeline pipeline;
	private Table queryTable;

	public TablesLabeledBasedMatcher(Pipeline pipeline, Table queryTable) {
		this.pipeline = pipeline;
		this.queryTable = queryTable;
		comparer = new LabelBasedComparer(pipeline, queryTable, true);

	}

	private class TableLabeledBasedMatcherThread implements Runnable {
		SPARQLEndpointQueryRunner queryRunner;
		private JoinResult joinResult;

		public TableLabeledBasedMatcherThread(JoinResult joinResult) {
			this.joinResult = joinResult;
		}

		public double matchTwoTables() {
			double completeScore = 0;

			String leftTableName = StringNormalizer.clearString(
					joinResult.getLeftTable(), false);
			String leftClean = comparer
					.cleanFileName(joinResult.getLeftTable());
			String rightClean = comparer.cleanFileName(joinResult
					.getRightTable());

			if (!comparer.namesToAvoid.contains(leftTableName)
					&& leftClean.length() > 2 && rightClean.length() > 2) {

				// check if the table headers are in the ontology of DBpedia or
				// YAGO
				completeScore += checkMatching(leftClean, rightClean);
			}

			// check if the column name is in the ontology of DBpedia or YAGO;
			// compared to the right table header
			completeScore += checkMatching(joinResult.getLeftColumn(),
					rightClean);

			// check if the column name is in the ontology of DBpedia or YAGO;
			// compared to the right table column header
			completeScore += checkMatching(joinResult.getLeftColumn(),
					joinResult.getRightColumn());

			// compare the rest of the columns headers
			completeScore += compareAllAttributes(joinResult.getRightTable());

			return completeScore;

		}

		private double checkMatching(String leftString, String rightString) {
			double completeScore = 0;
			int response = checkYAGOandDBpedia(leftString, rightString);
			if (response == 1)
				return completeScore + comparer.dbpediaAndYagoExactMatch;
			else if (response == -1)
				return completeScore - comparer.dbpediaAndYagoExactMatch;
			completeScore += comparer.matchStrings(leftString, rightString);
			return completeScore;
		}

		/**
		 * calculates the similarity between all headers from the left table and
		 * the right table
		 * 
		 * @param rightTableName
		 * @return
		 */
		private double compareAllAttributes(String rightTableName) {
			double completeScore = 0;

			NGramTokenizer tok = new NGramTokenizer(2, 4, true,
					new SimpleTokenizer(true, true));
			Jaccard sim = new Jaccard(tok);
			List<String> entityTableColumnHeaders = comparer
					.populateEntityTableHeaders(rightTableName);

			for (String leftColumn : comparer.queryTableColumnHeaders) {
				leftColumn = StringNormalizer.clearString(leftColumn, false);
				for (String rightColumn : entityTableColumnHeaders) {
					rightColumn = StringNormalizer.clearString(rightColumn,
							false);
					completeScore += sim.score(leftColumn, rightColumn);
				}
			}

			return completeScore
					/ (comparer.queryTableColumnHeaders.size() * entityTableColumnHeaders
							.size());
		}

		private int checkYAGOandDBpedia(String leftString, String rightString) {
			int DBpediaResponse = comparer.checkDBpediaClasses(leftString,
					rightString, queryRunner);
			if (DBpediaResponse == 1)
				return 1;
			else if (DBpediaResponse == -1)
				return -1;
			int YAGOresponse = comparer.checkYAGOClasses(leftString,
					rightString, queryRunner);
			if (YAGOresponse == 1)
				return 1;
			else if (YAGOresponse == -1)
				return -1;
			return 0;
		}

		public void run() {
			double score = matchTwoTables();

			joinResult.setLabelBasedSchemaMatchingRank(score);
		}
	}

	public static void main(String[] args) {
		// CopyOfTablesLabeledBasedMatcher mtc = new
		// CopyOfTablesLabeledBasedMatcher(
		// new Pipeline("", ""));
		//
		// JoinResult joinResult = new JoinResult();
		// joinResult.setLeftColumn("Film title");
		// joinResult.setRightColumn("core#prefLabel");
		// joinResult.setLeftTable("g3.gz");
		// joinResult.setRightTable("wordnet_video_games_103876519.csv.gz");
		// System.out.println(mtc.matchTwoTables(joinResult));

		// for (String n : NGram.getAllNgramsInBound("WATER OF BODU", 1, 3))
		// System.out.println(n);
		// System.out.println(NGram.getAllNgramsInBound("sss", 1, 3));
		// NGramTokenizer tok = new NGramTokenizer(2, 4, true,
		// new SimpleTokenizer(true, true));
		// Jaccard sim = new Jaccard(tok);
		// System.out.println(sim.score("nonPostalCode", "postalCode"));
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);
		System.out.println(sim.score("Ohrid", "Ohrid"));

	}
}
