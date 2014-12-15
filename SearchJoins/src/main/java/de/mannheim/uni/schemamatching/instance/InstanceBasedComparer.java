package de.mannheim.uni.schemamatching.instance;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.joda.time.DateTime;
import org.joda.time.Days;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.api.StringWrapper;
import com.wcohen.ss.expt.Blocker.Pair;
import com.wcohen.ss.expt.MatchData;
import com.wcohen.ss.expt.NGramBlocker;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnMatch;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableMatch;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.parsers.DateUtil;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.CalendarUtil;

public abstract class InstanceBasedComparer implements Runnable {

	public static TableMatch decideMatching(
			HashMap<String, HashMap<String, ColumnScoreValue>> scores,
			String table1, String table2) {
		// Determine final matching according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.4
		ArrayList<ColumnMatch> matches = new ArrayList<ColumnMatch>();
		HashSet<String> matchedCols = new HashSet<String>();
		for (String c1 : scores.keySet()) {
			double max = Double.MIN_VALUE;
			ColumnMatch m = new ColumnMatch();
			m.setColumn1(c1);

			// find the column with the highest matching score
			for (String c2 : scores.get(c1).keySet()) {
				ColumnScoreValue score = scores.get(c1).get(c2);

				if (score.getAverage() > max && !matchedCols.contains(c2)) {
					m.setColumn2(c2);
					m.setColumnType(score.getType());
					m.setScore(score.getAverage());
					m.setInstanceCount(score.getCount());
					max = score.getAverage();
				}
			}

			if (m.getColumn2() != null) {
				// use this match only, if it also has the highest matching
				// score for the second column
				for (String c1b : scores.keySet()) // iterate over all other
													// columns from the other
													// table
				{
					ColumnScoreValue score = scores.get(c1b)
							.get(m.getColumn2());

					if (score != null && score.getAverage() > max) // and check
																	// whether
																	// there is
																	// any other
																	// column
																	// with a
																	// higher
																	// score
					{
						// there is a better match for the second column, so do
						// not match it with c1
						m = null;
						break;
					}
				}

				if (m != null) {
					matchedCols.add(m.getColumn2());
					matches.add(m);
				}
			}
		}

		// aggregate column score to determine table score
		double ttlScore = 0;
		for (ColumnMatch m : matches) {
			ttlScore += m.getScore();
		}

		/*
		 * if(colCnt1==null) colCnt1=1; if(colCnt2==null) colCnt2=1;
		 */

		// normalize total score
		// ttlScore /= (double)Math.max(colCnt1, colCnt2);

		// Create and return TableMatch object
		TableMatch match = new TableMatch();
		match.setTable1(table1);
		match.setTable2(table2);
		match.setScore(ttlScore);
		match.setColumns(matches);

		return match;
	}

	/**
	 * @deprecated use Matcher.decide* instead
	 * Contains completes objects
	 * 
	 * @param scores
	 * @param table1
	 * @param table2
	 * @return
	 */
	@Deprecated 
	public static TableObjectsMatch decideObjectMatching(
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

	public class MinMax {
		public String Min;
		public String Max;
		public Date minDate;
		public Date maxDate;
	}

	protected MinMax getMinMaxValues(Map<Integer, String> col1Values,
			Map<Integer, String> col2Values, ColumnDataType dataType) {
		// ColumnDataType t = ColumnDataType.valueOf(dataType);
		ColumnDataType t = dataType;

		MinMax mm = new MinMax();

		Date dateMin = null, dateMax = null;

		// merge values
		Set<String> values = new HashSet<String>();
		for (String v : col1Values.values())
			values.add(v);
		for (String v : col2Values.values())
			values.add(v);

		// System.out.println("getMinMaxValues: checking " + values.size() +
		// " values of type " + t.toString() + " ...");

		// check values and determine min & max
		for (String value : values) {
			switch (t) {
			case coordinate:
			case unit:
			case numeric:
				/*
				 * try { double d = Double.parseDouble(value); } catch
				 * (Exception e) { // e.printStackTrace(); } break;
				 */
			case unknown:
			case string:
			case link:
			case bool:
				// for these types we don't need the range of values, so skip
				// this part
				return mm;
			case date:
				try {
					Date d = DateUtil.parse(value);

					if (d != null) {
						if (dateMin == null || d.before(dateMin)) {
							dateMin = d;
							mm.Min = value;
							mm.minDate = dateMin;
						}

						if (dateMax == null || d.after(dateMax)) {
							dateMax = d;
							mm.Max = value;
							mm.maxDate = dateMax;
						}
					}
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					// e.printStackTrace();
				}
				break;
			default:

			}
		}

		return mm;
	}

	protected double compareAllValues(TableColumn col1, TableColumn col2)
	{
		NGramBlocker blk = new NGramBlocker();
		blk.setMinNGramSize(2);
		blk.setMaxNGramSize(4);
		blk.setMaxFraction(0.1);
		
		// In clusterMode, consider pairings between instances from the same source. If clusterMode is false, only consider pairing between instances from different sources. 
		blk.setClusterMode(false);
		
		MatchData data = new MatchData();
		
		// determine index of last value for both columns
		int rows = 0;
		for(Integer i : col1.getValues().keySet())
			if(i>rows)
				rows = i;
		for(Integer i : col2.getValues().keySet())
			if(i>rows)
				rows = i;
		
		
		double pairCount=0;
		// iterate over all values
		for(int i=0; i<=rows;i++)
		{
			// add only those values that exists in both columns
			
			if(col1.getValues().containsKey(i) || col2.getValues().containsKey(i))
			{
				pairCount++; // if at least one values is not null, we count it for the average score!
				
				if(col1.getValues().containsKey(i) || col2.getValues().containsKey(i))
				{
					data.addInstance("1", Integer.toString(i), col1.getValues().get(i));
					data.addInstance("2", Integer.toString(i), col2.getValues().get(i));
				}
			}
		}
		
		blk.block(data);
		
		double totalScore=0;
		
		for(int i=0; i<blk.size(); i++)
		{
			Pair p = blk.getPair(i);
			
			if(p.isCorrect())
			{
				try {
					StringWrapper s1, s2;
					s1 = p.getA();
					s2 = p.getB();
					
					if (s1.length() <= 100 && s2.length() <= 100)
					{
						NGramTokenizer tok = new NGramTokenizer(2, 4, true,
							new SimpleTokenizer(true, true));
						Jaccard sim = new Jaccard(tok);
						totalScore += sim.score(s1, s2);
					}
					
					pairCount++;
				} catch (Exception e) {
					// e.printStackTrace();
				}
			}
		}
		
		return totalScore / pairCount;
		//return pairCount;
	}
	
	protected double compareColumnValues(String col1DataType, String col1Value,
			String col1Name, String col2DataType, String col2Value,
			String col2Name, MinMax minMaxValues) {
		double score = 0;

		ColumnDataType t;

		// if(e1.getColumnDataType().equals(e2.getColumnDataType()))
		// t = ColumnDataType.valueOf(e1.getColumnDataType());
		if (!col1DataType.equals(col2DataType)) // two columns of different type
												// are not at all similar
			return score;
		else
			t = ColumnDataType.valueOf(col1DataType);

		Timer tim = Timer.getNamed("compare column values", null);
		
		// handle some special cases
		if (col1Name.equals("22-rdf-syntax-ns#type")
				|| col1Name.equals("22-rdf-syntax-ns#type_label"))
			t = ColumnDataType.link; // for rdf types, always use exact matches

		// use exact match as default value for similarity score
		score = col1Value.equals(col2Value) ? 1 : 0;

		// Compare values according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.2.2

		switch (t) {
		case coordinate:
			// TODO implement comparing coordinate values, until then use
			// numeric similarity
		case unit:
		case numeric:
			Timer tNum = Timer.getNamed("numeric values", tim);
			try {
				String v1, v2;
				v1 = col1Value.replaceAll("[^0-9\\.\\,\\-]", "");
				v2 = col2Value.replaceAll("[^0-9\\.\\,\\-]", "");

				double value1 = Double.valueOf(v1);
				double value2 = Double.valueOf(v2);

				if (value1 == value2)
					score = 1.0;
				else
					score = 0.5 * Math.min(Math.abs(value1), Math.abs(value2))
							/ Math.max(Math.abs(value1), Math.abs(value2));
			} catch (Exception e) {
				// e.printStackTrace();
			}
			tNum.stop();
			break;
		case unknown:
		case string:
			Timer tString = Timer.getNamed("string values", tim);
			try {
				if (col1Value.length() <= 100 && col2Value.length() <= 100)
				{
					NGramTokenizer tok = new NGramTokenizer(2, 4, true,
						new SimpleTokenizer(true, true));
					Jaccard sim = new Jaccard(tok);
					score = sim.score(col1Value, col2Value);
				}
			} catch (Exception e) {
				// e.printStackTrace();
			}
			tString.stop();
			break;
		case date:
			Timer tDate = Timer.getNamed("date values", tim);
			try {
				Date d1 = DateUtil.parse(col1Value);
				Date d2 = DateUtil.parse(col2Value);

				// use min and max value of both columns to determine the value
				// range
				Date max = minMaxValues.maxDate;
				Date min = minMaxValues.minDate;
				
				int dateRange = Days.daysBetween(new DateTime(min), new DateTime(max)).getDays();
				int dateDiff = Math.abs(Days.daysBetween(new DateTime(d1), new DateTime(d2)).getDays());

				score = (double) dateDiff / (double) dateRange;
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			tDate.stop();
			break;
		case link:
			Timer tLink = Timer.getNamed("link values", tim);
			// This calculation is different from the one proposed in the paper,
			// as we do not deal with multiple links
			score = col1Value.equals(col2Value) ? 1 : 0;
			tLink.stop();
			break;
		case bool:
			Timer tBool = Timer.getNamed("boolean values", tim);
			Boolean b1, b2;

			if (col1Value.equalsIgnoreCase("true"))
				b1 = true;
			else if (col1Value.equalsIgnoreCase("false"))
				b1 = false;
			else
				b1=null;

			if (col2Value.equalsIgnoreCase("true"))
				b2 = true;
			else if (col2Value.equalsIgnoreCase("false"))
				b2 = false;
			else
				b2=null;

			if (b1 == b2 && b1!=null && b2!=null)
				score = 1;

			tBool.stop();
			break;
		default:

		}

		if (score > 1.0)
			System.out.println("Normalization error: " + col1Name + "("
					+ col1Value + ")" + " * " + col2Name + "(" + col2Value
					+ ")" + "[" + col1DataType + "] = " + score);

		tim.stop();
		return score;
	}

	/**
	 * determines the deviation of col1Value from col2Value
	 * 
	 * @param col1DataType
	 * @param col1Value
	 * @param col1Name
	 * @param col2DataType
	 * @param col2Value
	 * @param col2Name
	 * @param minMaxValues
	 * @return
	 */
	protected double getValueDeviation(String col1DataType, String col1Value,
			String col1Name, String col2DataType, String col2Value,
			String col2Name, MinMax minMaxValues) {
		double deviation = 1;

		ColumnDataType t;

		// if(e1.getColumnDataType().equals(e2.getColumnDataType()))
		// t = ColumnDataType.valueOf(e1.getColumnDataType());
		if (!col1DataType.equals(col2DataType)) // two columns of different type
												// are not at all similar
			return deviation;
		else
			t = ColumnDataType.valueOf(col1DataType);

		// handle some special cases
		if (col1Name.equals("22-rdf-syntax-ns#type")
				|| col1Name.equals("22-rdf-syntax-ns#type_label"))
			t = ColumnDataType.link; // for rdf types, always use exact matches

		// use exact match as default value for similarity score
		deviation = col1Value.equals(col2Value) ? 0 : 1;

		// Compare values according to
		// "Cross-lingual entity matching and infobox alignment in Wikipedia"
		// Section 6.2.2

		switch (t) {
		case coordinate:
			// TODO implement comparing coordinate values, until then use
			// numeric similarity
		case unit:
		case numeric:
			try {
				String v1, v2;
				v1 = col1Value.replaceAll("[^0-9\\.\\,\\-]", "");
				v2 = col2Value.replaceAll("[^0-9\\.\\,\\-]", "");

				double value1 = Double.parseDouble(v1);
				double value2 = Double.parseDouble(v2);

				double ratio = Math.min(Math.abs(value1), Math.abs(value2))
						/ Math.max(Math.abs(value1), Math.abs(value2));

				deviation = 1 - ratio;
			} catch (Exception e) {
				// e.printStackTrace();
			}
			break;
		case unknown:
		case string:
			// use exact match
			break;
		case date:
			try {
				Date d1 = DateUtil.parse(col1Value);
				Date d2 = DateUtil.parse(col2Value);

				// use min and max value of both columns to determine the value
				// range
				Date max = DateUtil.parse(minMaxValues.Max);
				Date min = DateUtil.parse(minMaxValues.Min);
				Calendar maxCal = CalendarUtil
						.getValidCalendar(max.getYear() + 1900,
								max.getMonth() + 1, max.getDate());
				Calendar minCal = CalendarUtil
						.getValidCalendar(min.getYear() + 1900,
								min.getMonth() + 1, min.getDate());
				// Use date difference in days
				int dateRange = CalendarUtil.elapsedDays(minCal, maxCal);

				Calendar before, after;
				if (d1.before(d2)) {
					before = CalendarUtil.getValidCalendar(d1.getYear() + 1900,
							d1.getMonth() + 1, d1.getDate());
					after = CalendarUtil.getValidCalendar(d2.getYear() + 1900,
							d2.getMonth() + 1, d2.getDate());
				} else {
					before = CalendarUtil.getValidCalendar(d2.getYear() + 1900,
							d2.getMonth() + 1, d2.getDate());
					after = CalendarUtil.getValidCalendar(d1.getYear() + 1900,
							d1.getMonth() + 1, d1.getDate());
				}

				int dateDiff = CalendarUtil.elapsedDays(before, after);

				deviation = 1 - ((double) dateDiff / (double) dateRange);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
			break;
		case link:
			break;
		case bool:
			boolean b1,
			b2;

			if (col1Value.equalsIgnoreCase("true"))
				b1 = true;
			else if (col1Value.equalsIgnoreCase("false"))
				b1 = false;
			else
				break;

			if (col2Value.equalsIgnoreCase("true"))
				b2 = true;
			else if (col2Value.equalsIgnoreCase("false"))
				b2 = false;
			else
				break;

			if (b1 == b2)
				deviation = 0;

			break;
		default:

		}

		return deviation;
	}
}