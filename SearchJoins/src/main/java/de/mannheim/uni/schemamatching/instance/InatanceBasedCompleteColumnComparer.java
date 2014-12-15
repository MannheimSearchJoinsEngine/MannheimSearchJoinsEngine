package de.mannheim.uni.schemamatching.instance;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.lucene.index.Term;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.concurrent.RunnableProgressReporter;

public class InatanceBasedCompleteColumnComparer {
	private Pipeline pipeline;

	public InatanceBasedCompleteColumnComparer(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	private Map<String, List<IndexEntry>> columnCash;

	private HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores;

	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> getScores() {
		return scores;
	}

	public void calculateScores(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores)
			throws InterruptedException {

		columnCash = new HashMap<String, List<IndexEntry>>();

		Timer tCol = new Timer("InstanceBasedCompleteColumnComparer for "
				+ instanceScores.size() * 2 + " columns");
		scores = new HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>>();

		ArrayList<CompleteColumnComparerThread> comparers = new ArrayList<CompleteColumnComparerThread>();
		ThreadPoolExecutor pool = new ThreadPoolExecutor(Runtime.getRuntime()
				.availableProcessors(), Runtime.getRuntime()
				.availableProcessors(), 0, TimeUnit.SECONDS,
				new java.util.concurrent.ArrayBlockingQueue<Runnable>(Math.max(
						instanceScores.size() * instanceScores.size() / 2, 1)));

		System.out.println("Checking " + instanceScores.size()
				+ " columns for duplicates:");

		HashMap<TableColumn, List<TableColumn>> passedColumns = new HashMap<TableColumn, List<TableColumn>>();

		for (Entry<TableColumn, HashMap<TableColumn, ColumnScoreValue>> entry : instanceScores
				.entrySet()) {
			for (Entry<TableColumn, ColumnScoreValue> entry2 : entry.getValue()
					.entrySet()) {
				// check if it is already computed
				if (passedColumns.containsKey(entry.getKey())) {
					if (passedColumns.get(entry.getKey()).contains(
							entry2.getKey())) {
						continue;
					}

				} else {

					passedColumns.put(entry.getKey(),
							new ArrayList<TableColumn>());
				}
				if (!passedColumns.containsKey(entry2.getKey()))
					passedColumns.put(entry2.getKey(),
							new ArrayList<TableColumn>());
				// add the values as passed for later
				passedColumns.get(entry.getKey()).add(entry2.getKey());
				passedColumns.get(entry2.getKey()).add(entry.getKey());

				CompleteColumnComparerThread cc = new CompleteColumnComparerThread(
						entry.getKey(), entry2.getKey(), entry2.getValue());

				comparers.add(cc);

				if (entry2.getValue().getComplementScore() > pipeline
						.getComplementaryThreshold()
						&& (entry.getKey().getDataSource() == null || !entry
								.getKey().getDataSource()
								.equals(entry2.getKey().getDataSource()))
						&& entry.getKey().getDataType()
								.equals(entry2.getKey().getDataType())) {
					pool.execute(cc);
				} else
					cc.setResults(null);
			}
		}

		RunnableProgressReporter p = new RunnableProgressReporter();
		p.setPool(pool);
		p.setTimer(tCol);
		p.start();

		pool.shutdown();

		pool.awaitTermination(1, TimeUnit.DAYS);
		p.stop();
		System.out.println("done.");
		tCol.stop();

		for (CompleteColumnComparerThread cc : comparers) {
			// if (cc.getResults() == null)
			// continue;
			if (!scores.containsKey(cc.getColumn1())) {
				scores.put(cc.getColumn1(),
						new HashMap<TableColumn, ColumnScoreValue>());
			}
			if (!scores.containsKey(cc.getColumn2())) {
				scores.put(cc.getColumn2(),
						new HashMap<TableColumn, ColumnScoreValue>());
			}

			scores.get(cc.getColumn1()).put(cc.getColumn2(), cc.getResults());
			scores.get(cc.getColumn2()).put(cc.getColumn1(), cc.getResults());

		}

	}

	public synchronized void putToMap(String key, List<IndexEntry> values) {
		columnCash.put(key, values);
	}

	public synchronized List<IndexEntry> getFromMap(String key) {
		if (columnCash.containsKey(key))
			return columnCash.get(key);
		return null;
	}

	public ColumnScoreValue matchColumns(TableColumn c1, TableColumn c2) {
		if (c1.getDataSource().equals(c2.getDataSource()))
			return null;
		Timer tim = Timer.getNamed("Match columns", null);

		Timer tLoad = Timer.getNamed("Load values", tim);

		// get the key entries of the table of the first column
		List<IndexEntry> column1KeyEntries = getFromMap(c1.getDataSource());
		if (column1KeyEntries == null) {
			column1KeyEntries = pipeline.getIndexManager().getValuesFromColumn(
					c1.getDataSource(), c1.getHeader(), true);
			putToMap(c1.getDataSource(), column1KeyEntries);
		}

		// // get the key entries of the table of the second column
		// List<IndexEntry> column2KeyEntries = getFromMap(c1.getDataSource());
		// if (column2KeyEntries == null) {
		// column2KeyEntries = pipeline.getIndexManager().getValuesFromColumn(
		// c2.getDataSource(), c2.getHeader(), true);
		// putToMap(c2.getDataSource(), column2KeyEntries);
		// }
		//
		// // get all entries of the first column
		// List<IndexEntry> column1Entries = getFromMap(c1.getDataSource() + "|"
		// + c1.getHeader());
		// if (column1Entries == null) {
		// column1Entries = pipeline.getIndexManager().getValuesFromColumn(
		// c1.getDataSource(), c1.getHeader(), false);
		// putToMap(c1.getDataSource() + "|" + c1.getHeader(), column1Entries);
		// }
		//
		// // get all entries of the second column
		// List<IndexEntry> column2Entries = getFromMap(c2.getDataSource() + "|"
		// + c2.getHeader());
		// if (column2Entries == null) {
		//
		// column2Entries = pipeline.getIndexManager().getValuesFromColumn(
		// c2.getDataSource(), c2.getHeader(), false);
		// putToMap(c2.getDataSource() + "|" + c2.getHeader(), column2Entries);
		// }

		// get the matching values of the second key column
		List<List<IndexEntry>> newKeyColumns = getMatchingKeyValues(
				column1KeyEntries, c2.getHeader(), c2.getDataSource());

		// copy the properties
		TableColumn tc1 = new TableColumn();
		tc1.setHeader(c1.getHeader());
		tc1.setDataSource(c1.getDataSource());
		tc1.setBaseUnit(c1.getBaseUnit());
		tc1.setDataType(c1.getDataType());

		getCorrespondingValuesToKeys(newKeyColumns.get(0), tc1);

		TableColumn tc2 = new TableColumn();
		tc2.setHeader(c2.getHeader());
		tc2.setDataSource(c2.getDataSource());
		tc2.setBaseUnit(c2.getBaseUnit());
		tc2.setDataType(c2.getDataType());

		getCorrespondingValuesToKeys(newKeyColumns.get(1), tc2);

		tLoad.stop();
		// NGramTokenizer tok = new NGramTokenizer(2, 4, true,
		// new SimpleTokenizer(true, true));
		// Jaccard sim = new Jaccard(tok);
		//
		// Timer tKey = Timer.getNamed("Find matching key values", tim);
		// // find if the key columns share same entries
		// int colsIndex = 1;
		// for (IndexEntry entry1 : column1KeyEntries) {
		// double maxScore = 0;
		// IndexEntry entry2Final = null;
		// for (IndexEntry entry2 : column2KeyEntries) {
		// double curScore = sim.score(entry1.getValue(),
		// entry2.getValue());
		// if (maxScore < curScore && curScore > 0.8) {
		// maxScore = curScore;
		// entry2Final = entry2;
		// if (maxScore == 1)
		// break;
		// }
		//
		// }
		// // add the matching entries' values in the columns
		// if (entry2Final != null) {
		// for (IndexEntry entryCol1 : column1Entries)
		// {
		// if (entryCol1.getEntryID() == entry1.getEntryID())
		// {
		// tc1.addNewValue(colsIndex, entryCol1.getValue(), true);
		// break;
		// }
		// }
		// for (IndexEntry entryCol2 : column2Entries)
		// {
		// if (entryCol2.getEntryID() == entry2Final.getEntryID())
		// {
		// tc2.addNewValue(colsIndex, entryCol2.getValue(), true);
		// break;
		// }
		// }
		//
		// colsIndex++;
		// }
		// }
		// tKey.stop();

		// calcuate the instance score;
		Timer tScore = Timer.getNamed("Calculate instance scores", tim);
		InstanceBasedColumnComparer comp = new InstanceBasedColumnComparer(pipeline);
		ColumnScoreValue sv = comp.compareColumns(tc1, tc2);
		tScore.stop();
		tim.stop();

		return sv;
	}

	/**
	 * gets the values of the column based on the key
	 * 
	 * @param keyColumn
	 * @param tc1
	 */
	private void getCorrespondingValuesToKeys(List<IndexEntry> keyColumn,
			TableColumn tc1) {
		List<TableColumn> result = new ArrayList<TableColumn>();
		// add the values from the first column
		int rowIndex = 0;

		for (IndexEntry en : keyColumn) {
			// search for the matching keys of the second column
			TermQuery categoryQuery = new TermQuery(new Term(
					IndexManager.FULL_TABLE_PATH, en.getFullTablePath()));
			TermQuery columnQuery = new TermQuery(new Term(
					IndexManager.columnHeader, en.getColumnHeader()));
			Query rowIdQuery = NumericRangeQuery.newIntRange(IndexManager.ID,
					1, en.getEntryID(), en.getEntryID(), true, true);

			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(columnQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(rowIdQuery, BooleanClause.Occur.MUST);
			List<IndexEntry> tmpList = pipeline.getIndexManager()
					.runQueryOnIndex(constrainedQuery, 1);
			if (tmpList.size() > 0) {
				tc1.addNewValue(rowIndex, tmpList.get(0).getValue(), true);
			}
			rowIndex++;
		}

	}

	/**
	 * returns the matching index entries based on the column
	 * 
	 * @param columnKeyEntries
	 * @param colHeader
	 * @param tableHeader
	 * @return
	 */
	private List<List<IndexEntry>> getMatchingKeyValues(
			List<IndexEntry> columnKeyEntries, String colHeader,
			String tableHeader) {
		List<List<IndexEntry>> results = new LinkedList<List<IndexEntry>>();
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);

		List<IndexEntry> entriesNewColumn = new LinkedList<IndexEntry>();
		List<IndexEntry> entriesFirstColumn = new LinkedList<IndexEntry>();

		for (IndexEntry en : columnKeyEntries) {
			// search for the matching keys of the second column
			TermQuery categoryQuery = new TermQuery(new Term(
					IndexManager.FULL_TABLE_PATH, tableHeader));
			TermQuery keyQuery = new TermQuery(new Term(
					IndexManager.IS_PRIMARY_KEY, "true"));
			Query q = null;
			try {
				q = parseQuery(en.getValue());
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				continue;
			}
			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(keyQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(q, BooleanClause.Occur.MUST);
			List<IndexEntry> tmpList = pipeline.getIndexManager()
					.runQueryOnIndex(constrainedQuery, 1);
			if (tmpList.size() > 0) {
				entriesNewColumn.add(tmpList.get(0));
				entriesFirstColumn.add(en);
			}

		}
		results.add(entriesFirstColumn);
		results.add(entriesNewColumn);
		return results;
	}

	public synchronized Query parseQuery(String value) throws ParseException {
		Query q = null;

		value = StringNormalizer.clearString(value, true);
		if (value.equals("")
				|| value.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
			throw new ParseException();
		value = pipeline.getIndexManager().getQueryParser().escape(value);
		q = pipeline.getIndexManager().getQueryParser().parse(value);

		return q;
	}

	private class CompleteColumnComparerThread implements Runnable {

		private TableColumn column1;
		private TableColumn column2;
		private ColumnScoreValue results;

		public void setResults(ColumnScoreValue results) {
			this.results = results;
		}

		public TableColumn getColumn1() {
			return column1;
		}

		public TableColumn getColumn2() {
			return column2;
		}

		public ColumnScoreValue getResults() {
			return results;
		}

		public CompleteColumnComparerThread(TableColumn column1,
				TableColumn column2, ColumnScoreValue results) {
			super();
			this.column1 = column1;
			this.column2 = column2;
			this.results = results;
		}

		public void run() {
			results = matchColumns(column1, column2);

		}
	}

	public static void main(String[] args) {
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);
		System.out.println(sim.score("canon eos 1 d x", "canon eos 1 d x"));
	}
}
