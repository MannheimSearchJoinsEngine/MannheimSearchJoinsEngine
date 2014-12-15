package de.mannheim.uni.searchjoin;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.logging.Level;

import com.mongodb.util.Args;
import com.mysql.jdbc.log.Log;
import com.sleepycat.je.rep.impl.RepGroupImpl.BarrierState;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.datafusion.DataFuser;
import de.mannheim.uni.datafusion.TableDataCleaner;
import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.index.infogather.KeyIndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.RankingType;
import de.mannheim.uni.schemamatching.instance.InstanceBasedMatcher;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.schemamatching.label.TablesLabeledBasedMatcher;
import de.mannheim.uni.scoring.ScoreEvaluator;
import de.mannheim.uni.statistics.EvaluatedJoinResult;
import de.mannheim.uni.statistics.FuseTableResultAnalyzer;
import de.mannheim.uni.statistics.SearchTableResultAnalyzer;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.FileUtils;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.RunnableQueueSizeReporter;
import de.mannheim.uni.utils.concurrent.Task;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;
import de.mannheim.uni.utils.FastJoinWrapper;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.ValueComparator;

/**
 * @author petar
 * 
 */
public class SearchJoin {
	private IndexManager indexManager;

	private Pipeline pipeline;

	public SearchJoin(Pipeline pipeline) {
		this.pipeline = pipeline;
		indexManager = pipeline.getIndexManager();
	}

	private void saveResults(String file, Map<String, JoinResult> results) {
		try {
			FileOutputStream fileOut = new FileOutputStream(file);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(results);
			out.close();
			fileOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private Map<String, JoinResult> loadResults(String file) {
		Map<String, JoinResult> results = null;

		try {
			FileInputStream fileIn = new FileInputStream(file);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			results = (Map<String, JoinResult>) in.readObject();
			in.close();
			fileIn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return results;
	}

	public Table searchJoinForTable(String tablePath) {
		Timer rootTimer = new Timer("SearchJoin");
		pipeline.getLogger().info("Start search join");

		long start = System.currentTimeMillis();
		Map<String, JoinResult> searchJoinResults = new HashMap<String, JoinResult>();

		Timer read = new Timer("Read query table");
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
		Table table = fileToTable.readTable(ReadTableType.search, tablePath);
		read.stop();

		// something is wrong with the table
		if (table == null)
			return null;

		// TableDataCleaner clean = new TableDataCleaner(table, pipeline);
		// table = clean.filterColumnsByColumnDensity(table);

		if (new File(tablePath + ".searchresults").exists()
				&& pipeline.isReuseSearchResults()) {
			pipeline.getLogger().info("Loading existing search results");
			Timer readSaved = new Timer("Load saved search results");
			searchJoinResults = loadResults(tablePath + ".searchresults");
			readSaved.stop();
		} else {
			Timer postSearch = new Timer("Post-processing search results");
			// read the allowed list based on the headers
			TreeSet<String> allowedTables = new TreeSet<String>();
			List<String> allowedHeaders = null;

			if (pipeline.getHeaderRefineAttrs().size() > 0) {
				allowedTables = FileUtils.readHeaderTablesFromFile(pipeline
						.getHeaderRefineAttrs().get(0));
			}
			if (pipeline.getHeaderRefineAttrs().size() > 1) {
				File f = new File(pipeline.getHeaderRefineAttrs().get(1));

				if (f.exists())
					try {
						allowedHeaders = org.apache.commons.io.FileUtils
								.readLines(f);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

			}
			for (TableColumn column : table.getColumns()) {
				if (column.isKey()) {
					// Map<String, JoinResult> searchJoinResultsTmp =
					// (findJoinsForColumn(column, table, null));
					Map<String, JoinResult> searchJoinResultsTmp = (findJoinsForColumnFast(
							column, table, null, allowedTables)); // ,
																	// allowedHeaders
					// checck refining attributes
					for (Entry<String, JoinResult> entry : searchJoinResultsTmp
							.entrySet()) {
						//
						// if (pipeline.getHeaderRefineAttrs().size() > 0
						// && !indexManager.filterByHeaders(entry.getValue()
						// .getJoinPairs().values().iterator().next()
						// .getFullTablePath(), 4,
						// pipeline.getHeaderRefineAttrs()))

						if (allowedTables.size() > 0)
							if (!checkEntryInList(allowedTables, entry
									.getValue().getJoinPairs().values()
									.iterator().next().getFullTablePath())) {
								continue;
							}
						searchJoinResults.put(entry.getKey(), entry.getValue());
					}
				}
			}
			postSearch.stop();
		}

		Timer saveSearch = new Timer("Save search results");
		saveResults(tablePath + ".searchresults", searchJoinResults);
		saveSearch.stop();

		// add here label based schema matching
		if (pipeline.isUseLabelBasedSchema()) {
			Timer lbl = new Timer("Label-baed schema matching");
			pipeline.getLogger().info("Starting label based matching");
			TablesLabeledBasedMatcher tb = new TablesLabeledBasedMatcher(
					pipeline, table);
			long startMatching = System.currentTimeMillis();
			tb.matchTables(searchJoinResults.values());
			double durMatching = ((double) (System.currentTimeMillis() - startMatching) / 1000);
			pipeline.getLogger().info("Label based matching:   " + durMatching);
			lbl.stop();
		}

		// actually, this is the first time the score for the JoinResults is
		// really used
		// so, all rank calculations are done here

		// first, determine max. values for normalization
		Timer rank = new Timer("Calculate ranking");
		double maxScore = Double.MIN_VALUE;
		int maxSize = Integer.MIN_VALUE;
		for (JoinResult r : searchJoinResults.values()) {
			double score = ScoreEvaluator.get(pipeline)
					.assessJoinResultLuceneScore(r);

			if (score > maxScore) {
				maxScore = score;
				maxSize = r.getJoinSize();
			}
		}
		System.out.println("Max score is " + maxScore + " for " + maxSize
				+ " join pairs.");

		for (JoinResult r : searchJoinResults.values()) {
			double score = ScoreEvaluator.get(pipeline).assessJoinResult(r,
					maxScore, maxSize);
			r.setTotalRank(score);
		}

		Map<String, Double> sortedResults = sortMap(searchJoinResults);
		ArrayList<JoinResult> results = new ArrayList<JoinResult>();

		int cnt = 0;

		for (Entry<String, Double> entry : sortedResults.entrySet()) {

			// pipeline.getLogger().info(searchJoinResults.get(entry.getKey()).print());
			System.out.println(searchJoinResults.get(entry.getKey()).print());

			if (pipeline.getRankingType() == RankingType.queryTableCoverageNormalized) {
				if (entry.getValue() >= 0.6)
					results.add(searchJoinResults.get(entry.getKey()));
			} else if (cnt++ < pipeline.getMaxMatchedTables()
					|| pipeline.getMaxMatchedTables() == 0)
				results.add(searchJoinResults.get(entry.getKey()));
		}
		rank.stop();

		try {
			JoinResult.writeCsv(results, "results."
					+ pipeline.getRankingType().toString() + "."
					+ pipeline.getKeyidentificationType().toString() + ".csv");
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		// write augmented table to file
		DataFuser fuser = new DataFuser(pipeline);
		Table fusedTable = fuser.fuseQueryTableWithEntityTables(table, results,
				null);

		long end = System.currentTimeMillis();

		double duration = ((double) (end - start) / 1000);
		if (pipeline.isUseInstanceBasedSchema()) {
			Timer inst = new Timer("Instance-based schema matching");
			pipeline.getLogger().info("Starting instance based matching");
			long startMatching = System.currentTimeMillis();
			InstanceBasedMatcher t = new InstanceBasedMatcher();
			try {
				t.matchResultTables(results, pipeline);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			double durMatching = ((double) (System.currentTimeMillis() - startMatching) / 1000);
			pipeline.getLogger()
					.info("Instance based matching: " + durMatching);
			inst.stop();
		}

		long startAnalysis = System.currentTimeMillis();

		// Analyze results
		// try {
		// Timer analyse = new Timer("Analyse results");
		//
		// // analyze results
		// Map<String, JoinResult> goldStandard = JoinResult
		// .readCsvMap(tablePath + ".gold.csv");
		// SearchTableResultAnalyzer a = new SearchTableResultAnalyzer();
		// List<EvaluatedJoinResult> evaluation = a.analyzeResult(table,
		// results, goldStandard, tablePath, pipeline, duration,
		// tablePath + ".wrongtables");
		// // write detailed results (for evaluation of the ranking methods)
		// EvaluatedJoinResult.writeDetailedCsv(evaluation, "results."
		// + pipeline.getRankingType().toString() + "."
		// + pipeline.getKeyidentificationType().toString()
		// + "_detailed.csv", false);
		//
		// // analyse fused values
		// Table refTable = fileToTable.readTable(ReadTableType.search, "ref."
		// + tablePath);
		// FuseTableResultAnalyzer fuseA = new FuseTableResultAnalyzer();
		// fuseA.analyzeResult(fusedTable, refTable);
		//
		// analyse.stop();
		// } catch (Exception ex) {
		// // System.out.println(ex.toString());
		// ex.printStackTrace();
		// }

		// double durAnalysis = ((double) (System.currentTimeMillis() -
		// startAnalysis) / 1000);

		pipeline.getLogger().info("Search join ended: " + duration);
		// pipeline.getLogger().info("Analysis of results: " + durAnalysis);

		rootTimer.stop();

		System.out.println(rootTimer.toString());

		return fusedTable;
	}

	/**
	 * chekcs if the entry contins some of the list entries
	 * 
	 * @param allowedTables
	 * @param fullTablePath
	 * @return
	 */
	private boolean checkEntryInList(TreeSet<String> allowedTables,
			String fullTablePath) {

		for (String tableName : allowedTables)
			if (fullTablePath.contains(tableName))
				return true;
		return false;
	}

	public static Map<String, Double> sortMap(
			Map<String, JoinResult> searchJoinResults) {
		Map<String, Double> rankedResults = new HashMap<String, Double>();

		for (Entry<String, JoinResult> entry : searchJoinResults.entrySet()) {
			rankedResults.put(entry.getKey(), entry.getValue().getTotalRank());
		}

		ValueComparator bvc = new ValueComparator(rankedResults);
		TreeMap<String, Double> sortedRankedResults = new TreeMap<String, Double>(
				bvc);
		sortedRankedResults.putAll(rankedResults);

		return sortedRankedResults;
	}

	public Set<String> printJoinTablesForColumnFast(final TableColumn column,
			final Table table) {
		final Timer tim = new Timer("FindJoinsForColumnFast");

		// Get entries of left table key column
		List<IndexEntry> indexEntries = TableManager.getEntriesForColumn(
				column, table, pipeline.getKeyidentificationType());

		final LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>> entryStream = new LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>>();
		final TreeSet<String> result = new TreeSet<String>();

		// for debugging:
		// Parallel.SetDefaultNumProcessors(1);
		RunnableQueueSizeReporter qr = new RunnableQueueSizeReporter(
				entryStream);
		qr.start();

		// start the grouping thread before the loop as Parallel.foreach is a
		// blocking call
		Task groupTask = new Task() {
			private boolean allDataLoaded() {
				return (Boolean) getUserData();
			}

			public void execute() {
				// Group search results by source table
				while (entryStream.size() > 0 || !allDataLoaded()) {
					Pair<IndexEntry, IndexEntry> p = entryStream.poll();
					if (p != null) {
						result.add(p.getSecond().getFullTablePath());
					}
				}
			}
		};
		groupTask.setUserData(false);
		Thread tGroup = Parallel.run(groupTask);

		System.out.println("Searching index ...");
		// Iterate over all key values
		try {
			new Parallel<IndexEntry>().foreach(indexEntries,
					new Consumer<IndexEntry>() {

						public void execute(IndexEntry parameter) {
							if (entryStream.size() > 10000000) {
								pipeline.getLogger()
										.log(Level.INFO,
												"Waiting for queue to be less full ...");
								while (entryStream.size() > 1000000)
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}

							}
							// Timer tSearch = Timer.getNamed("Search Index",
							// tim);
							List<IndexEntry> lst = searchIndex(parameter, null); // ,
																					// tables

							if (lst != null)
								for (IndexEntry rightEntry : lst)
									entryStream
											.add(new Pair<IndexEntry, IndexEntry>(
													parameter, rightEntry));

							// tSearch.stop();
						}

					}, tim);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Parallel.SetDefaultNumProcessors(Runtime.getRuntime()
				.availableProcessors());

		// wait for the grouping to finish
		groupTask.setUserData(true);
		System.out.println("Grouping data ...");
		try {
			tGroup.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		qr.stop();

		return result;
	}

	public Set<String[]> getKeyOverlap(final TableColumn column,
			final Table table) {
		final Timer tim = new Timer("FindJoinsForColumnFast");

		// Get entries of left table key column
		List<IndexEntry> indexEntries = TableManager.getEntriesForColumn(
				column, table, pipeline.getKeyidentificationType());

		final LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>> entryStream = new LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>>();
		final Set<String[]> result = new HashSet<String[]>();

		// for debugging:
		// Parallel.SetDefaultNumProcessors(1);
		RunnableQueueSizeReporter qr = new RunnableQueueSizeReporter(
				entryStream);
		qr.start();

		// start the grouping thread before the loop as Parallel.foreach is a
		// blocking call
		Task groupTask = new Task() {
			private boolean allDataLoaded() {
				return (Boolean) getUserData();
			}

			public void execute() {
				// Group search results by source table
				while (entryStream.size() > 0 || !allDataLoaded()) {
					Pair<IndexEntry, IndexEntry> p = entryStream.poll();
					if (p != null) {
						String[] values = new String[] {
								p.getSecond().getValue(),
								p.getSecond().getColumnDataType(),
								p.getSecond().getTabelHeader() };

						result.add(values);
					}
				}
			}
		};
		groupTask.setUserData(false);
		Thread tGroup = Parallel.run(groupTask);

		System.out.println("Searching index ...");
		// Iterate over all key values
		try {
			new Parallel<IndexEntry>().foreach(indexEntries,
					new Consumer<IndexEntry>() {

						public void execute(IndexEntry parameter) {
							if (entryStream.size() > 10000000) {
								pipeline.getLogger()
										.log(Level.INFO,
												"Waiting for queue to be less full ...");
								while (entryStream.size() > 1000000)
									try {
										Thread.currentThread().sleep(1000);
									} catch (InterruptedException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}

							}
							// Timer tSearch = Timer.getNamed("Search Index",
							// tim);
							List<IndexEntry> lst = searchIndex(parameter, null); // ,
																					// tables

							if (lst != null)
								for (IndexEntry rightEntry : lst)
									entryStream
											.add(new Pair<IndexEntry, IndexEntry>(
													parameter, rightEntry));

							// tSearch.stop();
						}

					}, tim);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Parallel.SetDefaultNumProcessors(Runtime.getRuntime()
				.availableProcessors());

		// wait for the grouping to finish
		groupTask.setUserData(true);
		System.out.println("Grouping data ...");
		try {
			tGroup.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		qr.stop();

		return result;
	}

	public Map<String, JoinResult> findJoinsForColumnFast(
			final TableColumn column, final Table table,
			final Map<String, Map<Integer, Double>> tsp,
			final TreeSet<String> tables) { // ,final List<String> headers
		final Timer tim = new Timer("FindJoinsForColumnFast");

		// Get entries of left table key column
		List<IndexEntry> indexEntries = TableManager.getEntriesForColumn(
				column, table, pipeline.getKeyidentificationType());

		final LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>> entryStream = new LinkedBlockingQueue<Pair<IndexEntry, IndexEntry>>();
		final HashMap<String, List<Pair<IndexEntry, IndexEntry>>> grouped = new HashMap<String, List<Pair<IndexEntry, IndexEntry>>>();
		final HashSet<String> keys = new HashSet<String>();

		// for debugging:
		Parallel.SetDefaultNumProcessors(1);
		RunnableQueueSizeReporter qr = new RunnableQueueSizeReporter(
				entryStream);
		qr.start();

		// start the grouping thread before the loop as Parallel.foreach is a
		// blocking call
		Task groupTask = new Task() {
			private boolean allDataLoaded() {
				return (Boolean) getUserData();
			}

			public void execute() {

				// BufferedWriter keyWriter = null;

				/*
				 * if(!pipeline.isSearchExactMatches()) try { keyWriter = new
				 * BufferedWriter(new FileWriter("found_keys.txt")); } catch
				 * (IOException e) { // TODO Auto-generated catch block
				 * e.printStackTrace(); }
				 */

				// Group search results by source table
				while (entryStream.size() > 0 || !allDataLoaded()) {
					Pair<IndexEntry, IndexEntry> p = entryStream.poll();
					if (p != null) {
						List<Pair<IndexEntry, IndexEntry>> lst = null;
						String rightHeader = p.getSecond().getTabelHeader();
						String checkHeader = rightHeader;

						if (checkHeader.endsWith("gz"))
							checkHeader = checkHeader.replace(".gz", "");

						if (tables == null || tables.size() == 0
								|| tables.contains(checkHeader)) {
							if (grouped.containsKey(rightHeader))
								lst = grouped.get(rightHeader);
							else {
								lst = new LinkedList<Pair<IndexEntry, IndexEntry>>();
								grouped.put(rightHeader, lst);
							}

							lst.add(p);

							if (!pipeline.isSearchExactMatches()) // &&
																	// keyWriter!=null)
							{
								String value = p.getSecond().getValue();
								keys.add(value);
							}
							/*
							 * try {
							 * 
							 * //value = StringNormalizer.clearString(value,
							 * false, pipeline.isSearchStemmedKeys()); //value =
							 * value.replaceAll("\\P{InBasic_Latin}", "");
							 * //value = value.substring(0,
							 * Math.min(value.length(), 127)); //value =
							 * StringNormalizer.clearString4FastJoin(value,
							 * true, pipeline.isSearchStemmedKeys());
							 * keyWriter.write(value + "\n");
							 * 
							 * } catch (IOException e) { // TODO Auto-generated
							 * catch block e.printStackTrace(); }
							 */
						}
					}
				}

				/*
				 * if(!pipeline.isSearchExactMatches() && keyWriter!=null) try {
				 * keyWriter.close(); } catch (IOException e) { // TODO
				 * Auto-generated catch block e.printStackTrace(); }
				 */
			}
		};
		groupTask.setUserData(false);
		Thread tGroup = Parallel.run(groupTask);

		// System.out.println("Searching index ...");
		pipeline.getLogger().info("Searching index ...");
		// Iterate over all key values
		try {
			new Parallel<IndexEntry>(2).foreach(indexEntries,
					new Consumer<IndexEntry>() {

						public void execute(IndexEntry parameter) {
							// Timer tSearch = Timer.getNamed("Search Index",
							// tim);
							List<IndexEntry> lst = searchIndex(parameter, tsp); // ,
																				// headers,
																				// tables

							if (lst != null)
								for (IndexEntry rightEntry : lst) {
									entryStream
											.add(new Pair<IndexEntry, IndexEntry>(
													parameter, rightEntry));
								}

							// tSearch.stop();
						}

					}, tim);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		Parallel.SetDefaultNumProcessors(Runtime.getRuntime()
				.availableProcessors());

		// wait for the grouping to finish
		groupTask.setUserData(true);
		// System.out.println("Grouping data ...");
		pipeline.getLogger().info("Grouping data ...");
		try {
			tGroup.join();
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		qr.stop();

		// System.out.println("Preparing join ...");
		pipeline.getLogger().info("Preparing join ...");
		final Map<String, JoinResult> result = new ConcurrentHashMap<String, JoinResult>();

		Set<String> matchedKeys0 = null;

		if (!pipeline.isSearchExactMatches()) {
			// System.out.println("Matching keys ...");
			pipeline.getLogger().info("Matching keys ...");

			final ConcurrentLinkedQueue<String> normalisedKeys = new ConcurrentLinkedQueue<String>();

			try {
				new Parallel<String>().foreach(keys, new Consumer<String>() {

					public void execute(String parameter) {
						String value = parameter;
						value = StringNormalizer.clearString4FastJoin(value,
								true, pipeline.isSearchStemmedKeys());

						normalisedKeys.add(value);
					}
				});
			} catch (InterruptedException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				BufferedWriter keyWriter = new BufferedWriter(new FileWriter(
						"found_keys.txt"));

				for (String s : normalisedKeys)
					keyWriter.write(s + "\n");

				keyWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			HashSet<String> qKeys = new HashSet<String>();
			try {
				BufferedWriter queryKeys = new BufferedWriter(new FileWriter(
						"query_keys.txt"));
				for (String key : column.getValues().values()) {
					String value = StringNormalizer.clearString(key, false,
							pipeline.isSearchStemmedKeys());
					queryKeys.write(value + "\n");
					qKeys.add(value);
				}
				queryKeys.close();
			} catch (Exception e) {
				e.printStackTrace();
			}

			FastJoinWrapper fj = new FastJoinWrapper();
			matchedKeys0 = fj.match("found_keys.txt", "query_keys.txt",
					pipeline);

			if (matchedKeys0 == null || matchedKeys0.size() == 0) {
				// System.out.println("FastJoin failed!");
				pipeline.getLogger().info("FastJoin failed!");
				return result;
			}

			// don't forget to add the keys from the query table!
			matchedKeys0.addAll(qKeys);
		}

		final Set<String> matchedKeys = matchedKeys0;

		// prepare the column header filter (make sure it is loaded)
		pipeline.getHeaderFilter();

		try {
			new Parallel<List<Pair<IndexEntry, IndexEntry>>>().foreach(
					grouped.values(),
					new Consumer<List<Pair<IndexEntry, IndexEntry>>>() {

						public void execute(
								List<Pair<IndexEntry, IndexEntry>> parameter) {
							List<Pair<IndexEntry, IndexEntry>> values = parameter;

							if (values.size() > 0) {
								IndexEntry leftEntry = values.get(0).getFirst();
								IndexEntry rightEntry = values.get(0)
										.getSecond();

								if (leftEntry == null || rightEntry == null)
									return;

								// filter out tables that have no matching
								// column header
								// if we filter out by headers here, we cannot
								// re-use the search results for another query
								// over the same set of keys ...
								// if(!indexManager.hasHeader(rightEntry.getTabelHeader(),
								// pipeline.getHeaderFilter()))
								// return ;

								String tmpHheader = table.getHeader() + "|"
										+ column.getHeader() + "|"
										+ rightEntry.getTabelHeader() + "|"
										+ rightEntry.getColumnHeader();

								JoinResult jr = new JoinResult();

								jr.setHeader(tmpHheader);
								jr.setLeftColumn(column.getHeader());
								jr.setLeftColumnCardinality(column
										.getTotalSize());
								jr.setLeftColumnDistinctValues(column
										.getValuesInfo().size());
								jr.setLeftTable(table.getHeader());

								jr.setRightColumn(leftEntry.getColumnHeader());
								jr.setRightColumncardinality(leftEntry
										.getTableCardinality());
								jr.setRightColumnDistinctValues(leftEntry
										.getColumnDistinctValues());
								jr.setRightTable(leftEntry.getTabelHeader());

								for (Pair<IndexEntry, IndexEntry> v : values) {
									leftEntry = v.getFirst();
									rightEntry = v.getSecond();

									if (!pipeline.isSearchExactMatches()) {
										String value = rightEntry.getValue();
										value = StringNormalizer.clearString4FastJoin(
												value, true,
												pipeline.isSearchStemmedKeys());
										if (!matchedKeys.contains(value)) {
											System.out
													.println(value
															+ " does not match any key!");
											continue;
										}
									}

									jr.setCount(jr.getCount() + 1);
									jr.setLeftSumMultiplicity(jr
											.getLeftSumMultiplicity()
											+ leftEntry.getValueMultiplicity());
									jr.setRightSumMultiplicity(jr
											.getRightSumMultiplicity()
											+ rightEntry.getValueMultiplicity());
									jr.setJoinSize(jr.getJoinSize()
											+ (leftEntry.getValueMultiplicity() * rightEntry
													.getValueMultiplicity()));

									jr.getJoinPairs()
											.put(leftEntry, rightEntry);
								}

								if (jr.getJoinPairs().size() > 0)
									result.put(tmpHheader, jr);
							}
						}

					});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		tim.stop();
		return result;
	}

	private List<IndexEntry> searchIndex(IndexEntry leftEntry,
			Map<String, Map<Integer, Double>> tsp) // , List<String> headers,
													// List<String> tables
	{
		KeyIndexManager im = null;
		if (tsp != null)
			im = new KeyIndexManager(pipeline);

		List<IndexEntry> rightEntries = null;
		// use the standard index or the infoGather
		if (tsp == null) {
			/*
			 * if(tables!=null && tables.size()>0) { rightEntries = new
			 * LinkedList<IndexEntry>();
			 * 
			 * for(String s : tables) { List<IndexEntry> results =
			 * indexManager.searchIndex(leftEntry, s);
			 * 
			 * rightEntries.addAll(results); } } else
			 */
			rightEntries = indexManager.searchIndex(leftEntry);
		} else {
			// TODO: this can be optimized by adding all tables names in the
			// query
			// retrieve all entries
			rightEntries = im.searchIndex(leftEntry.getValue());
			// remove the entries not coming from this tables
			List<IndexEntry> entriesToremove = new LinkedList<IndexEntry>();
			for (IndexEntry indeEntry : rightEntries) {
				boolean toDelete = true;
				for (Entry<String, Map<Integer, Double>> entry : tsp.entrySet()) {
					if (indeEntry.getTabelHeader().equals(entry.getKey())) {
						toDelete = false;
						break;
					}
				}
				if (toDelete)
					entriesToremove.add(indeEntry);
			}
			// remove the unused entries
			for (IndexEntry en : entriesToremove)
				rightEntries.remove(en);
		}

		return rightEntries;
	}

	public Map<String, JoinResult> findJoinsForColumn(TableColumn column,
			Table table, Map<String, Map<Integer, Double>> tsp) {
		// initialize the infogather index if needed
		KeyIndexManager im = null;
		if (tsp != null)
			im = new KeyIndexManager(pipeline);
		Map<String, JoinResult> columnJoinResults = new HashMap<String, JoinResult>();

		List<IndexEntry> indexEntries = TableManager.getEntriesForColumn(
				column, table, pipeline.getKeyidentificationType());
		for (IndexEntry leftEntry : indexEntries) {
			// if the value is null we are not interested in the results
			if (leftEntry.getValue()
					.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
				continue;
			List<IndexEntry> rightEntries = null;
			// use the standard index or the infoGather
			if (tsp == null)
				rightEntries = indexManager.searchIndex(leftEntry);
			else {
				// TODO: this can be optimized by adding all tables names in the
				// query
				// retrieve all entries
				rightEntries = im.searchIndex(leftEntry.getValue());
				// remove the entries not coming from this tables
				List<IndexEntry> entriesToremove = new LinkedList<IndexEntry>();
				for (IndexEntry indeEntry : rightEntries) {
					boolean toDelete = true;
					for (Entry<String, Map<Integer, Double>> entry : tsp
							.entrySet()) {
						if (indeEntry.getTabelHeader().equals(entry.getKey())) {
							toDelete = false;
							break;
						}
					}
					if (toDelete)
						entriesToremove.add(indeEntry);
				}
				// remove the unused entries
				for (IndexEntry en : entriesToremove)
					rightEntries.remove(en);
			}
			// used to avoid multiple matches from one column, E.g. "owl" from
			// leftColumn "label" might match "owl" from rightColumn "label" and
			// "black owl" from the same rightColumn "label"; We don't want to
			// add them both, but only the first one, which has higher rank
			// the docs are sorted by rank
			List<String> tmpPassedJoins = new ArrayList<String>();
			for (IndexEntry rightEntry : rightEntries) {
				// used to see if the first entry was not the best match
				boolean newEntryIsUsed = false;
				IndexEntry rightTmp = null;

				double entryScore;
				entryScore = ScoreEvaluator.get(pipeline).assessIndexEntry(
						rightEntry);

				String tmpHheader = table.getHeader() + "|"
						+ column.getHeader() + "|"
						+ rightEntry.getTabelHeader() + "|"
						+ rightEntry.getColumnHeader();
				if (tmpPassedJoins.contains(tmpHheader)) {
					// this is used to resolve the problem with the avoided
					// documentsNorms in DBIndex
					// it should be removed when the index is completed
					// correctly
					JoinResult joinResulttmp = columnJoinResults
							.get(tmpHheader);
					rightTmp = joinResulttmp.getJoinPairs().get(leftEntry);

					double tmpScore;
					tmpScore = ScoreEvaluator.get(pipeline).assessIndexEntry(
							rightTmp);

					if (tmpScore < entryScore
							|| (tmpScore == entryScore && rightEntry.getValue()
									.length() < rightTmp.getValue().length())) {
						newEntryIsUsed = true;
					} else {
						continue;
					}
				} else {
					tmpPassedJoins.add(tmpHheader);
				}

				// create the joinResult
				JoinResult joinResult = new JoinResult();
				if (!columnJoinResults.containsKey(tmpHheader)) {
					// set left
					joinResult.setHeader(tmpHheader);
					joinResult.setLeftColumn(column.getHeader());
					joinResult.setLeftColumnCardinality(column.getTotalSize());
					joinResult.setLeftColumnDistinctValues(column
							.getValuesInfo().size());
					joinResult.setLeftTable(table.getHeader());
					// set right
					joinResult.setRightColumn(rightEntry.getColumnHeader());
					joinResult.setRightColumncardinality(rightEntry
							.getTableCardinality());
					joinResult.setRightColumnDistinctValues(rightEntry
							.getColumnDistinctValues());
					joinResult.setRightTable(rightEntry.getTabelHeader());

				} else {
					joinResult = columnJoinResults.get(tmpHheader);
				}
				if (!newEntryIsUsed) {
					joinResult.setCount(joinResult.getCount() + 1);
					joinResult.setLeftSumMultiplicity(joinResult
							.getLeftSumMultiplicity()
							+ leftEntry.getValueMultiplicity());
					joinResult.setRightSumMultiplicity(joinResult
							.getRightSumMultiplicity()
							+ rightEntry.getValueMultiplicity());
					joinResult.setJoinSize(joinResult.getJoinSize()
							+ (leftEntry.getValueMultiplicity() * rightEntry
									.getValueMultiplicity()));
				} else {
					joinResult.setRightSumMultiplicity(joinResult
							.getRightSumMultiplicity()
							- rightTmp.getValueMultiplicity()
							+ rightEntry.getValueMultiplicity());
					joinResult.setJoinSize(joinResult.getJoinSize()
							- (leftEntry.getValueMultiplicity() * rightTmp
									.getValueMultiplicity())
							+ (leftEntry.getValueMultiplicity() * rightEntry
									.getValueMultiplicity()));
				}

				// add the pair
				joinResult.getJoinPairs().put(leftEntry, rightEntry);
				columnJoinResults.put(tmpHheader, joinResult);

			}
		}

		return columnJoinResults;
	}
}
