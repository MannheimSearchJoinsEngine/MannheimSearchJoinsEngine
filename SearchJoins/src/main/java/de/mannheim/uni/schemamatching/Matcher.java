package de.mannheim.uni.schemamatching;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.MatchingScores;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.MatchClustering.Linkage;
import de.mannheim.uni.schemamatching.instance.InatanceBasedCompleteColumnComparer;
import de.mannheim.uni.schemamatching.instance.InstanceBasedMatcher;
import de.mannheim.uni.schemamatching.label.TablesLabeledBasedMatcher;
import de.mannheim.uni.scoring.ScoreEvaluator;
import de.mannheim.uni.statistics.Timer;

public class Matcher {

	private Pipeline pipe;
	private String watchColumn;

	private double getCombinedScore(ColumnScoreValue instanceScore,
			ColumnScoreValue labelScore, ColumnScoreValue comInstaceScore) {
		return ScoreEvaluator.get(pipe).getFinalColumnSimilarity(instanceScore,
				labelScore, comInstaceScore);
	}

	public Matcher(Pipeline pipeline) {
		watchColumn = "population";
		pipe = pipeline;
	}

	private MatchingScores loadSimilarities(String fileName) {
		MatchingScores results = null;

		try {
			FileInputStream fileIn = new FileInputStream(fileName);
			ObjectInputStream in = new ObjectInputStream(fileIn);
			results = (MatchingScores) in.readObject();
			in.close();
			fileIn.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return results;
	}

	private void saveSimilarities(MatchingScores data, String fileName) {
		try {
			FileOutputStream fileOut = new FileOutputStream(fileName);
			ObjectOutputStream out = new ObjectOutputStream(fileOut);
			out.writeObject(data);
			out.close();
			fileOut.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public TableObjectsMatch matchColumns(Table table)
			throws InterruptedException {
		Timer tim = new Timer("Matcher");

		HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores = null;
		HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores = null;
		HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> completeInstanceScores = null;

		if (!pipe.isReuseColumnSimilarities()) {
			System.out.println("Running instance-based matching");
			InstanceBasedMatcher m = new InstanceBasedMatcher();
			m.calculateScores(table, pipe);
			instanceScores = m.getScores();

			System.out.println("Running label-based matching");
			TablesLabeledBasedMatcher lm = new TablesLabeledBasedMatcher(pipe,
					table);
			lm.calculateScores(table);
			labelScores = lm.getScores();

			MatchingScores ms = new MatchingScores();
			ms.instanceScores = instanceScores;
			ms.labelScores = labelScores;
			ms.completeInstanceScores = null;
			if (pipe.isUseCompleteComparer()) {
				System.out
						.println("Running instance-based matching on complete columns");
				InatanceBasedCompleteColumnComparer icc = new InatanceBasedCompleteColumnComparer(
						pipe);
				icc.calculateScores(instanceScores);
				completeInstanceScores = icc.getScores();
				ms.completeInstanceScores = completeInstanceScores;
			}

			saveSimilarities(ms, "matcher_similarities.bin");

			writeScores(instanceScores, labelScores, completeInstanceScores);
		} else {
			MatchingScores ms = loadSimilarities("matcher_similarities.bin");

			instanceScores = ms.instanceScores;
			labelScores = ms.labelScores;

			completeInstanceScores = ms.completeInstanceScores;
		}

		System.out.println("Deciding matching");
		TableObjectsMatch result = decideCombinedObjectMatching(instanceScores,
				labelScores, completeInstanceScores, table);

		tim.stop();

		return result;
	}

	public TableObjectsMatch clusterColumns(Table table) throws Exception {
		Timer tim = new Timer("Matcher");

		HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores = null;
		HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores = null;

		if (!pipe.isReuseColumnSimilarities()) {
			System.out.println("Running instance-based matching");
			InstanceBasedMatcher m = new InstanceBasedMatcher();
			m.calculateScores(table, pipe);
			instanceScores = m.getScores();

			System.out.println("Running label-based matching");
			TablesLabeledBasedMatcher lm = new TablesLabeledBasedMatcher(pipe,
					table);
			lm.calculateScores(table);
			labelScores = lm.getScores();

			MatchingScores ms = new MatchingScores();
			ms.instanceScores = instanceScores;
			ms.labelScores = labelScores;

			saveSimilarities(ms, "matcher_similarities.bin");

			writeScores(instanceScores, labelScores, null);
		} else {
			System.out.println("Loading similarity values ...");
			MatchingScores ms = loadSimilarities("matcher_similarities.bin");

			instanceScores = ms.instanceScores;
			labelScores = ms.labelScores;
		}

		System.out.println("Deciding matching");
		TableObjectsMatch result = decideMatchingClusters(instanceScores,
				labelScores, table);

		tim.stop();

		return result;
	}

	public TableObjectsMatch decideMatchingClusters(
			final HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores,
			final HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores,
			Table table) throws Exception {
		Timer tim = new Timer("Decide matching clusters");
		MatchClustering<TableColumn> c = new MatchClustering<TableColumn>() {

			@Override
			public double getNormalizedSimilarity(TableColumn object1,
					TableColumn object2) {

				ColumnScoreValue inst = instanceScores.get(object1)
						.get(object2);
				ColumnScoreValue lbl = labelScores.get(object1).get(object2);

				return getCombinedScore(inst, lbl, null);
			}

			@Override
			public String getKey(TableColumn value) {
				return value.getHeader() + value.getDataSource();
			}

			@Override
			public boolean canMerge(MatchCluster<TableColumn> cluster1,
					MatchCluster<TableColumn> cluster2, double similarity) {

				double t = 0;

				if (cluster1.getCluster().get(0).getDataType() == ColumnDataType.string)
					t = pipe.getDuplicateLimitInstanceString();
				else
					t = pipe.getDuplicateLimitInstanceNumeric();

				return similarity > t;
			}

		};

		List<MatchCluster<TableColumn>> clusters = c
				.clusterMatchesAgglomerative(new ArrayList<TableColumn>(
						instanceScores.keySet()), Linkage.MinDist);

		TableObjectsMatch result = new TableObjectsMatch();
		ArrayList<ColumnObjectsMatch> columnMatches = new ArrayList<ColumnObjectsMatch>();

		for (MatchCluster<TableColumn> cluster : clusters) {
			if (cluster.getCluster().size() > 1) {
				ColumnObjectsMatch cm = new ColumnObjectsMatch();

				cm.setColumn1(cluster.getCluster().get(0));
				cm.setColumnType(cluster.getCluster().get(0).getDataType()
						.toString());
				cm.setScore(cluster.getSimilarity());

				for (int i = 1; i < cluster.getCluster().size(); i++) {
					cm.getColumn2().add(cluster.getCluster().get(i));
				}

				columnMatches.add(cm);
			}
		}

		result.setColumns(columnMatches);

		tim.stop();
		return result;
	}

	public TableObjectsMatch matchColumnsComplementary(Table table)
			throws InterruptedException {
		return decideComplementaryObjectMatching(matchInstances(table), table);
	}

	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> matchLabels(
			Table table) throws InterruptedException {
		System.out.println("Running label-based matching");
		TablesLabeledBasedMatcher lm = new TablesLabeledBasedMatcher(pipe,
				table);
		lm.calculateScores(table);
		return lm.getScores();
	}

	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> matchInstances(
			Table table) throws InterruptedException {
		System.out.println("Running instance-based matching");
		InstanceBasedMatcher m = new InstanceBasedMatcher();
		m.calculateScores(table, pipe);
		return m.getScores();
	}

	public TableObjectsMatch decideLabelMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			Table table) {
		return decideObjectMatching(scores, table.getHeader(),
				table.getHeader(), pipe.getDuplicateLimitInstanecLabelString(),
				pipe.getDuplicateLimitInstanecLabelNumeric());
	}

	public TableObjectsMatch decideInstanceMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			Table table) {
		return decideObjectMatching(scores, table.getHeader(),
				table.getHeader(), pipe.getDuplicateLimitInstanceString(),
				pipe.getDuplicateLimitInstanceNumeric());
	}

	public TableObjectsMatch decideObjectMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			String table1, String table2, double stringThreshold,
			double numericThreshold) {
		Timer t = new Timer("Decide matching");
		// Determine final matching according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.4
		ArrayList<ColumnObjectsMatch> matches = new ArrayList<ColumnObjectsMatch>();
		HashSet<TableColumn> matchedCols = new HashSet<TableColumn>();
		for (TableColumn c1 : scores.keySet()) {
			if (!matchedCols.contains(c1)) {
				double max = Double.MIN_VALUE;
				ColumnObjectsMatch m = new ColumnObjectsMatch();
				m.setColumn1(c1);
				TableColumn firstMatch = null;

				// find the column with the highest matching score
				for (TableColumn c2 : scores.get(c1).keySet()) {
					ColumnScoreValue score = scores.get(c1).get(c2);

					if (score == null) {
						System.out.println("Column combination '"
								+ c1.getHeader() + "' and '" + c2.getHeader()
								+ "' does not have a score! (1)");
					} else {
						if (score.getAverage() > max
								&& !matchedCols.contains(c2)) {
							// m.setColumn2(c2);
							// m.getColumn2().add(c2);
							firstMatch = c2;
							m.setColumnType(score.getType());
							m.setScore(score.getAverage());
							m.setInstanceCount(score.getCount());
							max = score.getAverage();

							if (c2.getHeader().contains("population")
									|| c1.getHeader().contains("population"))
								System.out.println("Initial matching: "
										+ c1.getHeader() + " + "
										+ c2.getHeader() + " (" + max + ")");
						}
					}
				}

				if (firstMatch != null)
					m.getColumn2().add(firstMatch);
				else
					continue;
				// currentMatches.add(m);

				// find all columns with the same matching score (multiple
				// matches)

				// use threshold instead of maximum score
				if (c1.getDataType() == ColumnDataType.string)
					max = Math.min(max, stringThreshold);
				else
					max = Math.min(max, numericThreshold);
				System.out.println("Score threshold is " + max);

				// add all other columns which also have high score values
				for (TableColumn c2 : scores.get(c1).keySet()) {
					ColumnScoreValue score = scores.get(c1).get(c2);

					if (score == null) {
						System.out.println("Column combination '"
								+ c1.getHeader() + "' and '" + c2.getHeader()
								+ "' does not have a score! (2)");
					} else {
						if (score.getAverage() >= max
								&& !matchedCols.contains(c2)
								&& !m.getColumn2().contains(c2)) {
							m.getColumn2().add(c2);
							if (c2.getHeader().contains("population")
									|| c1.getHeader().contains("population"))
								System.out.println("Additional matching: "
										+ c1.getHeader() + " + "
										+ c2.getHeader() + "("
										+ score.getAverage() + ")");
						}
					}
				}

				if (m.getColumn2().size() > 0) {
					// System.out.println("Trying to match " + c1.getHeader() +
					// " (" + c1.getDataSource() + ") [" + max + "]");

					Iterator<TableColumn> it = m.getColumn2().iterator();

					while (it.hasNext()) {
						TableColumn c2 = it.next();

						ColumnScoreValue currentScore = scores.get(c1).get(c2);
						double currentAvg = 0.0;
						if (currentScore != null)
							currentAvg = currentScore.getAverage();

						// use this match only, if it also has the highest
						// matching
						// score for the second column
						for (TableColumn c1b : scores.keySet()) // iterate over
																// all
																// other
						// columns from the other
						// table
						{
							// don't remove the match if the better column is
							// also matched ...
							if (!m.getColumn2().contains(c1b)) {
								ColumnScoreValue score = scores.get(c1b)
										.get(c2);

								if (score == null) {
									System.out.println("Column combination '"
											+ c1.getHeader() + "' and '"
											+ c2.getHeader()
											+ "' does not have a score! (3)");
								} else {
									if (score != null
											&& score.getAverage() > currentAvg) // and
									// check
									// whether
									// there
									// is
									// any
									// other
									// column
									// with
									// a
									// higher
									// score
									{
										// there is a better match for the
										// second
										// column, so do
										// not match it with c1
										// System.out
										// .println(" ... but there's a better match with "
										// + c1b.getHeader()
										// + " ["
										// + score.getAverage() + "]");
										if (c2.getHeader().contains(
												"population")
												|| c1.getHeader().contains(
														"population"))
											System.out.println("removed "
													+ c2.getHeader()
													+ " from matching ("
													+ c1b.getHeader() + " ["
													+ score.getAverage()
													+ "] is better)");
										it.remove();
										break;
									}
								}
							}
						}
						// System.out.println("... OK");
						// there is no better match, keep it

					} // while(it.hasNext())
					if (m.getColumn2().size() > 0) {

						if (c1.getHeader().contains("population")) {
							System.out.print("Final matching "
									+ m.getColumn1().getHeader() + " + ");
							for (TableColumn tc : m.getColumn2())
								System.out.print(tc.getHeader() + ", ");
							System.out.println();
						}

						// there is at least one column left, so this is a match
						matches.add(m);
						matchedCols.add(m.getColumn1()); // keep track of
															// already matched
															// columns

						for (TableColumn c2 : m.getColumn2()) {
							if (c2.getHeader().contains("population"))
								System.out.println("Matched " + c2.getHeader());

							matchedCols.add(c2); // keep track of already
													// matched
													// columns
						}

						if (m.getColumn1().getHeader().contains("population"))
							System.out.println("Matched "
									+ m.getColumn1().getHeader());
					}
				} // if (m.getColumn2() != null) {
			}
		} // for

		// aggregate column score to determine table score
		double ttlScore = 0;
		for (ColumnObjectsMatch m : matches) {
			ttlScore += m.getScore();
		}

		/*
		 * if(colCnt1==null) colCnt1=1; if(colCnt2==null) colCnt2=1;
		 */

		// normalize total score
		// ttlScore /= (double)Math.max(colCnt1, colCnt2);

		// Create and return TableMatch object
		TableObjectsMatch match = new TableObjectsMatch();
		match.setTable1(table1);
		match.setTable2(table2);
		match.setScore(ttlScore);
		match.setColumns(matches);

		t.stop();
		return match;
	}

	public TableObjectsMatch decideCombinedObjectMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> completeInstaceScores,
			Table table) {
		return decideCombinedObjectMatching(instanceScores, labelScores,
				completeInstaceScores, table.getHeader(), table.getHeader(),
				pipe.getDuplicateLimitInstanceString(),
				pipe.getDuplicateLimitInstanceNumeric());
	}

	public TableObjectsMatch decideCombinedObjectMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> completeInstaceScores,
			String table1, String table2, double stringThreshold,
			double numericThreshold) {
		Timer t = new Timer("Decide matching");
		// Determine final matching according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.4
		ArrayList<ColumnObjectsMatch> matches = new ArrayList<ColumnObjectsMatch>();
		HashSet<TableColumn> matchedCols = new HashSet<TableColumn>();

		for (TableColumn c1 : instanceScores.keySet()) {
			if (!matchedCols.contains(c1)) {
				double threshold = 0.0;
				if (c1.getDataType() == ColumnDataType.string)
					threshold = stringThreshold;
				else
					threshold = numericThreshold;

				double max = Double.MIN_VALUE;
				ColumnObjectsMatch m = new ColumnObjectsMatch();
				m.setColumn1(c1);
				TableColumn firstMatch = null;

				// find the column with the highest matching score
				for (TableColumn c2 : instanceScores.get(c1).keySet()) {
					ColumnScoreValue instScore = instanceScores.get(c1).get(c2);
					ColumnScoreValue lblScore = labelScores.get(c1).get(c2);

					// get the complete column instance score
					ColumnScoreValue comInstaceScore = null;
					if (completeInstaceScores != null)
						comInstaceScore = completeInstaceScores.get(c1).get(c2);
					double s = getCombinedScore(instScore, lblScore,
							comInstaceScore);

					if (s > max && s >= threshold && !matchedCols.contains(c2)) {
						// m.setColumn2(c2);
						// m.getColumn2().add(c2);
						firstMatch = c2;
						m.setColumnType(instScore.getType());
						m.setScore(s);
						m.setInstanceCount(instScore.getCount());
						max = s;
					}
				}

				if (firstMatch != null)
					m.getColumn2().add(firstMatch);
				else {
					if (c1.getHeader().contains(watchColumn))
						System.out.println("No match for " + c1.getHeader());
					continue;
				}
				// currentMatches.add(m);

				// find all columns with the same matching score (multiple
				// matches)

				// use threshold instead of maximum score
				// unless max score is lower than the threshold
				if (c1.getDataType() == ColumnDataType.string)
					max = stringThreshold;
				else
					max = numericThreshold;

				if (c1.getHeader().contains(watchColumn)
						|| firstMatch.getHeader().contains(watchColumn))
					System.out.println("Matched " + c1.getHeader() + " with "
							+ firstMatch.getHeader());

				System.out.println("Score threshold is " + max);

				// add all other columns which also have high score values
				for (TableColumn c2 : instanceScores.get(c1).keySet()) {
					ColumnScoreValue instScore = instanceScores.get(c1).get(c2);
					ColumnScoreValue lblScore = labelScores.get(c1).get(c2);
					// get the complete column instance score
					ColumnScoreValue comInstaceScore = null;
					if (completeInstaceScores != null)
						comInstaceScore = completeInstaceScores.get(c1).get(c2);

					double score = getCombinedScore(instScore, lblScore,
							comInstaceScore);

					if (score >= max && !matchedCols.contains(c2)
							&& !m.getColumn2().contains(c2)) {
						m.getColumn2().add(c2);

						if (c1.getHeader().contains(watchColumn)
								|| c2.getHeader().contains(watchColumn))
							System.out.println("Additionally matched "
									+ c1.getHeader() + " with "
									+ c2.getHeader() + "[" + score + "]");
					}
				}

				// check for better matches of candidate columns
				if (m.getColumn2().size() > 0) {
					Iterator<TableColumn> it = m.getColumn2().iterator();

					while (it.hasNext()) {
						TableColumn c2 = it.next();

						ColumnScoreValue instScore = instanceScores.get(c1)
								.get(c2);
						ColumnScoreValue lblScore = labelScores.get(c1).get(c2);
						// get the complete column instance score
						ColumnScoreValue comInstaceScore = null;
						if (completeInstaceScores != null)
							comInstaceScore = completeInstaceScores.get(c1)
									.get(c2);
						double currentAvg = getCombinedScore(instScore,
								lblScore, comInstaceScore);

						// use this match only, if it also has the highest
						// matching score for the second column
						// iterate over all other columns from the other table
						for (TableColumn c1b : instanceScores.keySet()) {
							// don't remove the match if the better column is
							// also matched ...
							if (!m.getColumn2().contains(c1b)) {
								ColumnScoreValue instScore2 = instanceScores
										.get(c1).get(c2);
								ColumnScoreValue lblScore2 = labelScores
										.get(c1).get(c2);
								// get the complete column instance score
								ColumnScoreValue comInstaceScore2 = null;
								if (completeInstaceScores != null)
									comInstaceScore = completeInstaceScores
											.get(c1).get(c2);

								double score = getCombinedScore(instScore2,
										lblScore2, comInstaceScore2);

								// and check whether there is any other column
								// with a higher score
								if (score > currentAvg) {
									if (c1.getHeader().contains(watchColumn)
											|| c2.getHeader().contains(
													watchColumn))
										System.out.println("Removed matched "
												+ c1.getHeader() + " with "
												+ firstMatch.getHeader() + " ("
												+ c1b.getHeader()
												+ " is better)" + "[" + score
												+ "]");

									// there is a better match for the second
									// column, so do not match it with c1
									it.remove();
									break;
								}
							}
						}
						// there is no better match, keep it
					} // while(it.hasNext())
					if (m.getColumn2().size() > 0) {
						// there is at least one column left, so this is a match
						matches.add(m);
						matchedCols.add(m.getColumn1()); // keep track of
															// already matched
															// columns

						for (TableColumn c2 : m.getColumn2()) {
							matchedCols.add(c2); // keep track of already
													// matched
													// columns
						}
					}
				} // if (m.getColumn2() != null) {
			}
		} // for

		// aggregate column score to determine table score
		double ttlScore = 0;
		for (ColumnObjectsMatch m : matches) {
			ttlScore += m.getScore();
		}

		/*
		 * if(colCnt1==null) colCnt1=1; if(colCnt2==null) colCnt2=1;
		 */

		// normalize total score
		// ttlScore /= (double)Math.max(colCnt1, colCnt2);

		// Create and return TableMatch object
		TableObjectsMatch match = new TableObjectsMatch();
		match.setTable1(table1);
		match.setTable2(table2);
		match.setScore(ttlScore);
		match.setColumns(matches);

		t.stop();
		return match;
	}

	/**
	 * Decides a matching based on ColumnScoreValue.getMixedScore
	 * 
	 * @param scores
	 * @param table
	 * @return
	 */
	public TableObjectsMatch decideComplementaryObjectMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			Table table) {
		return decideComplementaryObjectMatching(scores, table.getHeader(),
				table.getHeader(), pipe.getDuplicateLimitInstanceString(),
				pipe.getDuplicateLimitInstanceNumeric(),
				pipe.getComplementaryScore());
	}

	/**
	 * Decides a matching based on ColumnScoreValue.getMixedScore
	 * 
	 * @param scores
	 * @param table1
	 * @param table2
	 * @param stringThreshold
	 * @param numericThreshold
	 * @param complementScore
	 * @return
	 */
	public TableObjectsMatch decideComplementaryObjectMatching(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			String table1, String table2, double stringThreshold,
			double numericThreshold, double complementScore) {
		Timer t = new Timer("Decide matching (mixed)");
		ScoreEvaluator se = ScoreEvaluator.get(pipe);
		// Determine final matching according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.4
		ArrayList<ColumnObjectsMatch> matches = new ArrayList<ColumnObjectsMatch>();
		HashSet<TableColumn> matchedCols = new HashSet<TableColumn>();

		for (TableColumn c1 : scores.keySet()) {
			if (!matchedCols.contains(c1)) {
				double threshold = 0.0;
				if (c1.getDataType() == ColumnDataType.string)
					threshold = stringThreshold;
				else
					threshold = numericThreshold;

				double max = Double.MIN_VALUE;
				ColumnObjectsMatch m = new ColumnObjectsMatch();
				m.setColumn1(c1);
				TableColumn firstMatch = null;

				// find the column with the highest matching score
				for (TableColumn c2 : scores.get(c1).keySet()) {
					ColumnScoreValue score = scores.get(c1).get(c2);

					double s = se.getComplementaryColumnSimilarity(score);

					if (s > max && s > threshold && !matchedCols.contains(c2)) {
						// m.setColumn2(c2);
						// m.getColumn2().add(c2);
						firstMatch = c2;
						m.setColumnType(score.getType());
						m.setScore(s);
						m.setInstanceCount(score.getCount());
						max = s;
					}
				}

				if (firstMatch != null)
					m.getColumn2().add(firstMatch);
				else {
					if (c1.getHeader() != null
							&& c1.getHeader().contains(watchColumn))
						System.out.println("No match for " + c1.getHeader());
					continue;
				}
				// currentMatches.add(m);

				// find all columns with the same matching score (multiple
				// matches)

				// use threshold instead of maximum score
				// unless max score is lower than the threshold
				if (c1.getDataType() == ColumnDataType.string)
					max = stringThreshold;
				else
					max = numericThreshold;

				if (c1.getHeader().contains(watchColumn)
						|| firstMatch.getHeader().contains(watchColumn))
					System.out.println("Matched " + c1.getHeader() + " with "
							+ firstMatch.getHeader());

				System.out.println("Score threshold is " + max);

				// add all other columns which also have high score values
				for (TableColumn c2 : scores.get(c1).keySet()) {
					ColumnScoreValue sv = scores.get(c1).get(c2);

					double score = se.getComplementaryColumnSimilarity(sv);

					if (score >= max && !matchedCols.contains(c2)
							&& !m.getColumn2().contains(c2)) {
						m.getColumn2().add(c2);

						if (c1.getHeader().contains(watchColumn)
								|| c2.getHeader().contains(watchColumn))
							System.out.println("Additionally matched "
									+ c1.getHeader() + " with "
									+ c2.getHeader() + "[" + score + "]");
					}
				}

				// check for better matches of candidate columns
				if (m.getColumn2().size() > 0) {
					Iterator<TableColumn> it = m.getColumn2().iterator();

					while (it.hasNext()) {
						TableColumn c2 = it.next();

						ColumnScoreValue sv = scores.get(c1).get(c2);

						double currentAvg = se
								.getComplementaryColumnSimilarity(sv);

						// use this match only, if it also has the highest
						// matching score for the second column
						// iterate over all other columns from the other table
						for (TableColumn c1b : scores.keySet()) {
							// don't remove the match if the better column is
							// also matched ...
							if (!m.getColumn2().contains(c1b)) {
								ColumnScoreValue sv2 = scores.get(c1).get(c2);

								double score = se
										.getComplementaryColumnSimilarity(sv2);

								// and check whether there is any other column
								// with a higher score
								if (score > currentAvg) {
									if (c1.getHeader().contains(watchColumn)
											|| c2.getHeader().contains(
													watchColumn))
										System.out.println("Removed matched "
												+ c1.getHeader() + " with "
												+ firstMatch.getHeader() + " ("
												+ c1b.getHeader()
												+ " is better)" + "[" + score
												+ "]");

									// there is a better match for the second
									// column, so do not match it with c1
									it.remove();
									break;
								}
							}
						}
						// there is no better match, keep it
					} // while(it.hasNext())
					if (m.getColumn2().size() > 0) {
						// there is at least one column left, so this is a match
						matches.add(m);
						matchedCols.add(m.getColumn1()); // keep track of
															// already matched
															// columns

						for (TableColumn c2 : m.getColumn2()) {
							matchedCols.add(c2); // keep track of already
													// matched
													// columns
						}
					}
				} // if (m.getColumn2() != null) {
			}
		} // for

		// aggregate column score to determine table score
		double ttlScore = 0;
		for (ColumnObjectsMatch m : matches) {
			ttlScore += m.getScore();
		}

		/*
		 * if(colCnt1==null) colCnt1=1; if(colCnt2==null) colCnt2=1;
		 */

		// normalize total score
		// ttlScore /= (double)Math.max(colCnt1, colCnt2);

		// Create and return TableMatch object
		TableObjectsMatch match = new TableObjectsMatch();
		match.setTable1(table1);
		match.setTable2(table2);
		match.setScore(ttlScore);
		match.setColumns(matches);

		t.stop();
		return match;
	}

	private void writeScores(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores,
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> comInstanceScores) {
		try {
			BufferedWriter log = new BufferedWriter(new FileWriter(
					"combinedmatching.csv"));

			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tInstance Similarity\tComplementarity\tLabel Similarity\tcomplete Instance similarity\tCombined Similarity\n");

			ScoreEvaluator se = ScoreEvaluator.get(pipe);

			for (Entry<TableColumn, HashMap<TableColumn, ColumnScoreValue>> en : instanceScores
					.entrySet()) {

				for (Entry<TableColumn, ColumnScoreValue> w : en.getValue()
						.entrySet()) {
					try {
						ColumnScoreValue inst = w.getValue();
						ColumnScoreValue lbl = labelScores.get(en.getKey())
								.get(w.getKey());

						ColumnScoreValue cin = null;
						if (comInstanceScores != null)
							cin = comInstanceScores.get(en.getKey()).get(
									w.getKey());

						log.write(en.getKey().getHeader() + "\t");
						log.write(en.getKey().getDataSource() + "\t");
						log.write(en.getKey().getDataType() + "\t");
						log.write(w.getKey().getHeader() + "\t");
						log.write(w.getKey().getDataSource() + "\t");
						log.write(w.getValue().getAverage() + "\t");
						log.write(w.getValue().getComplementScore() + "\t");
						log.write(se.getNormalizedLabelScoreForFusion(lbl
								.getAverage()) + "\t");
						String completeScore = "/";
						if (cin != null)
							completeScore = Double.toString(cin.getAverage());
						log.write(completeScore + "\t");
						log.write(getCombinedScore(inst, lbl, cin) + "");
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

	public void printMatchingResult(
			HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores,
			double complementScore, String filename) {
		try {
			ScoreEvaluator se = ScoreEvaluator.get(pipe);
			BufferedWriter log = new BufferedWriter(new FileWriter(filename));

			log.write("Column 1\tColumn 1 Data Source\tColumn Data Type\tColumn 2\tColumn 2 Data Source\tSimilarity\tComplementarity\tMixed Score\tComplement count\tvalues count\ttotal count\n");

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
						log.write(w.getValue().getAverage() + "\t");
						log.write(w.getValue().getComplementScore() + "\t");
						log.write(se.getComplementaryColumnSimilarity(w
								.getValue()) + "\t");
						log.write(w.getValue().getComplementCount() + "\t");
						log.write(w.getValue().getCount() + "\t");
						log.write(w.getValue().getTotalCount() + "");
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

	public void printTableObjectsMatch(TableObjectsMatch m, String filename) {
		try {
			BufferedWriter log = new BufferedWriter(new FileWriter(filename));

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
	
	public static TableObjectsMatch readTableObjectsMatch(String filename)
	{
		TableObjectsMatch m = new TableObjectsMatch();
		
		try {
			BufferedReader r = new BufferedReader(new FileReader(filename));
			
			// skip header
			r.readLine();
			
			String line;
			
			List<ColumnObjectsMatch> matches = new LinkedList<ColumnObjectsMatch>();
			
			while((line = r.readLine()) != null)
			{
				String[] values = line.split("\t");
				
				TableColumn c = getTableColumnFromTableObjectsMatchString(values[0]);
				
				ColumnObjectsMatch cm = findColumnObjectsMatchInList(matches, c.getHeader());
				
				if(cm==null)
				{
					cm = new ColumnObjectsMatch();
					cm.setColumn1(c);
					List<TableColumn> lst = new LinkedList<TableColumn>();
					cm.setColumn2(lst);
					
					double score = Double.parseDouble(values[5]);
					cm.setScore(score);
					
					matches.add(cm);
				}

				c = getTableColumnFromTableObjectsMatchString(values[3]);
				cm.getColumn2().add(c);
				
			}
			
			m.setColumns(matches);
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return m;
		
	}
	
	private static ColumnObjectsMatch findColumnObjectsMatchInList(List<ColumnObjectsMatch> list, String header)
	{
		for(ColumnObjectsMatch cm : list)
			if(cm.getColumn1().getHeader().equals(header))
				return cm;
		return null;
	}
	
	private static TableColumn getTableColumnFromTableObjectsMatchString(String value)
	{
		TableColumn c = new TableColumn();
		
		Pattern p = Pattern.compile("(.*)\\(/(.*)\\) (\\d+)");
		
		java.util.regex.Matcher m = p.matcher(value);
		
		if(m.matches())
		{
			c.setHeader(m.group(1) + " " + m.group(3));
			c.setDataSource(m.group(2));
			c.setCleaningInfo(value);
		}
		else
			c.setHeader(value);
		
		return c;
	}
}
