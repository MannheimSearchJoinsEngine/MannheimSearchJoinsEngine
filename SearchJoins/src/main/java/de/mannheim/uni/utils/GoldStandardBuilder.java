package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.TableProcessor.TableReader;
import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.scoring.ScoreEvaluator;
import de.mannheim.uni.statistics.SearchTableResultAnalyzer;

public class GoldStandardBuilder {

	public static void main(String[] args) throws IOException {
		if (args.length < 4) {
			System.out
					.println("Tool for creating a gold standard from a search result.");
			System.out.println("Parameters:");
			System.out.println("1. Search Result file name (.csv)");
			System.out.println("2. Gold Standard output file (.csv)");
			System.out.println("3. Index location");
			System.out.println("4. query table file name");
			System.out.println("5. (Optional) entity table name");
			return;
		}

		GoldStandardBuilder b = new GoldStandardBuilder();
		String tableName = args.length == 5 ? args[4] : "";

		if (tableName.equals("?")) {
			System.out.println("Enter name of table to check: ");
			BufferedReader reader = new BufferedReader(new InputStreamReader(
					System.in));

			tableName = reader.readLine();
		}

		b.buildGoldStandard(args[0], args[1], args[2], tableName, args[3]);
	}

	private long start, tableStart;
	private long matchesEvaluated;
	private List<JoinResult> goldStandard = null;
	private List<IndexEntry> queryTableEntries;
	private IndexManager indexManager;

	public void buildGoldStandard(String resultFileName,
			String goldStandardFile, String indexLocation, String table,
			String queryTablePath) {
		start = System.currentTimeMillis();
		matchesEvaluated = 0;
		// read the query Table
		Pipeline p = new Pipeline("BuildGoldStandardPipeline", indexLocation);
		p.setKeyidentificationType(KeyIdentificationType.singleWithRefineAttrs);
		indexManager = new IndexManager(p);
		ConvertFileToTable fileToTable = new ConvertFileToTable(p);
		Table queryTable = fileToTable.readTable(ReadTableType.search,
				queryTablePath);

		for (TableColumn column : queryTable.getColumns())
			if (column.isKey())
				queryTableEntries = TableManager
						.getEntriesForColumn(column, queryTable,
								KeyIdentificationType.singleWithRefineAttrs);

		System.out.println("Query Table entries:");
		for (IndexEntry e : queryTableEntries) {
			System.out.println("\t" + e.getValue());
		}

		List<JoinResult> results = null;
		try {
			// Open results file
			results = JoinResult.readCsv(resultFileName);
			System.out.println("Found " + results.size() + " matched tables.");
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
			return;
		}

		// List<JoinResult> goldStandard = null;

		try {
			// try to initialize gold standard with values from last session
			goldStandard = JoinResult.readCsv(goldStandardFile);
			// System.out.println("Found existing gold standard. Skipping " +
			// goldStandard.size() + " tables already evaluated.");
			System.out.println("Found existing gold standard.");

			for (JoinResult g : goldStandard) {
				Iterator<JoinResult> it = results.iterator();

				while (it.hasNext()) {
					JoinResult r = it.next();

					if (r.getRightTable().equals(g.getRightTable())) {
						System.out.println("Skipping " + r.getRightTable());
						it.remove();
						break;
					}
				}
			}
		} catch (Exception ex) {
			System.out
					.println("No existing gold standard found, creating new one.");
			goldStandard = new ArrayList<JoinResult>();
		}

		if (table.equals("")) {
			// for each matched entity table
			for (int index = 0; index < results.size(); index++) {
				tableStart = System.currentTimeMillis();
				JoinResult r = results.get(index);

				// process table only if the following conditions apply
				// 1) no other table from the same directory using the same key
				// column has been evaluated
				boolean dirAlreadyEvaluated = false;
				for (JoinResult test : goldStandard) {
					// Determine existing table file location
					Set<Entry<IndexEntry, IndexEntry>> set = test
							.getJoinPairs().entrySet();
					Entry<IndexEntry, IndexEntry> entry = set.iterator().next();
					String tblPath = entry.getValue().getFullTablePath();
					File f = new File(tblPath);

					String path = f.getAbsolutePath();
					if (f.isFile())
						path = f.getParent();

					// Determine new table file location
					f = new File(r.getJoinPairs().entrySet().iterator().next()
							.getValue().getFullTablePath());

					String path2 = f.getAbsolutePath();
					if (f.isFile())
						path2 = f.getParent();

					if (path.equals(path2)
							&& test.getRightColumn().equals(r.getRightColumn())) {
						dirAlreadyEvaluated = true;
						break;
					}
				}
				if (!dirAlreadyEvaluated) {
					File f = new File(r.getJoinPairs().entrySet().iterator()
							.next().getValue().getFullTablePath());

					String path = f.getAbsolutePath();
					if (f.isFile())
						path = f.getParent();

					System.out.println("New directory: " + path);
				}

				// or 2) the table contains new matches that are missing in the
				// gold standard
				boolean newMatches = getNewMatches(r, true).size() > 0;

				if (!dirAlreadyEvaluated || newMatches)
					if (processTable(r, index, results.size(), indexLocation,
							goldStandardFile, queryTablePath + ".wrongtables"))
						break;
			} // for(int index=goldStandard.size();index<results.size();index++)
		} else {
			// Only check one specific table
			JoinResult r = null;

			for (int i = 0; i < results.size(); i++) {
				if (results.get(i).getRightTable().equals(table))
					r = results.get(i);
			}

			if (r == null) {
				System.out.println("Table " + table + " not found in results!");
				return;
			}

			tableStart = System.currentTimeMillis();

			processTable(r, 0, 1, indexLocation, goldStandardFile,
					queryTablePath + ".wrongtables");
		}
	}

	private List<Entry<IndexEntry, IndexEntry>> getNewMatches(JoinResult r,
			boolean silent) {
		List<Entry<IndexEntry, IndexEntry>> result = new ArrayList<Map.Entry<IndexEntry, IndexEntry>>();

		for (Entry<IndexEntry, IndexEntry> entry : r.getJoinPairs().entrySet()) {
			if (isNewMatch(entry.getKey(), entry.getValue(), silent)
					&& !result.contains(entry))
				result.add(entry);
		}

		return result;
	}

	private boolean isNewMatch(IndexEntry key, IndexEntry value, boolean silent) {
		boolean valueExists = false;
		boolean keyExists = false;

		for (JoinResult test : goldStandard) {
			Entry<IndexEntry, IndexEntry> e = SearchTableResultAnalyzer
					.getEntry(test.getJoinPairs(), key.getValue());

			if (e != null) {
				// the current key is not missing in the gold standard
				keyExists = true;

				// the current value is contained in the gold standard
				if (e.getValue().getValue().equalsIgnoreCase(value.getValue())) {
					valueExists = true;
					break;
				}
			}
		}

		if (!keyExists) {
			if (!silent)
				System.out.println("New key value: " + formatQueryValue(key)
						+ " --> " + formatQueryValue(value));
			return true;
		}

		if (!valueExists) {
			if (!silent)
				System.out.println("New match: " + formatQueryValue(key)
						+ " --> " + formatQueryValue(value));
			return true;
		}

		return false;
	}

	private void clearScreen() {
		for (int i = 0; i < 30; i++)
			System.out.println();
	}

	private List<IndexEntry> getTableRow(String tableName, int rowId)
	{
		return indexManager.getRowValues(tableName, rowId);
	}
	
	private String[] getValuesFromRow(List<IndexEntry> row)
	{
		String[] values = new String[row.size()];
		
		for(int i=0;i<row.size();i++)
			values[i] = row.get(i).getValue();

		return values;
	}
	
	private String[] getHeadersFromRow(List<IndexEntry> row)
	{
		String[] values = new String[row.size()];
		
		for(int i=0;i<row.size();i++)
			values[i] = row.get(i).getColumnHeader();

		return values;		
	}

	private boolean processTable(JoinResult r, int resultIndex,
			int resultCount, String indexLocation, String goldStandardFile,
			String wrongTablesFile) {
		boolean exit = false;
		boolean rejectTable = false;

		// Determine table file location
		String table = r.getJoinPairs().entrySet().iterator().next().getValue()
				.getFullTablePath();

		if (!table.endsWith(".gz")) {
			File f = new File(table);
			table = new File(f, r.getRightTable()).getAbsolutePath();
		}

		// Open table for reading
		//TableReader reader = new TableReader(table);

		System.out.println("\n\nResult of table " + r.getRightTable()
				+ "(table no. " + (resultIndex + 1) + "/" + resultCount + "): "
				+ r.getJoinPairs().size() + " matches.");
		
		// BufferedReader reader = null;
		try {
			// List<Entry<IndexEntry, IndexEntry>> entries =
			// sortEntries(r.getJoinPairs().entrySet());
			List<Entry<IndexEntry, IndexEntry>> lst = sortEntries(getNewMatches(
					r, false));

			// remove all matches that are not new to the gold standard
			Iterator<Entry<IndexEntry, IndexEntry>> it = r.getJoinPairs()
					.entrySet().iterator();
			while (it.hasNext()) {
				boolean isNew = false;

				String key = it.next().getKey().getValue();

				for (Entry<IndexEntry, IndexEntry> e : lst) {
					if (key.equals(e.getKey().getValue())) {
						isNew = true;
						break;
					}
				}

				if (!isNew)
					it.remove();
			}

			/*if (lst.size() != 0)
				reader.open();*/

			int matchIndex = 0;
			// int numMatches = r.getJoinPairs().size();
			int numMatches = lst.size();

			boolean first = true;
			boolean endTable = false;
			// for(Entry<IndexEntry, IndexEntry> e : entries)
			for (Entry<IndexEntry, IndexEntry> e : lst) {
				if (endTable) {
					// end table evaluation = skip all remaining matches
					r.getJoinPairs().remove(e.getKey());
					continue;
				}

				if (!first)
					clearScreen();

				first = false;

				IndexEntry queryKey = getEntryForKey(e.getKey().getValue());

				/*String[] values = reader.GetEntryValues(e.getValue()
						.getEntryID());*/
				
				String tableHeader = r.getRightTable();
				//String tableHeader = e.getValue().getTabelHeader();
				System.out.println("Loading values for record " + e.getValue().getEntryID() + " from table " + tableHeader + " ...");
				List<IndexEntry> row = getTableRow(tableHeader, e.getValue().getEntryID());
				String[] headers = getHeadersFromRow(row);
				String[] values = getValuesFromRow(row);

				if (values == null)
					break; // end of file reached
				else {
					StringBuilder eval = new StringBuilder();

					eval.append("Column headers:\n");
					for(String h : headers)
						eval.append(h + "; ");
					
					eval.append("\nFull row for query key '"
							+ formatQueryValue(queryKey) + "':\n");
					// print complete line of entity table
					for (String v : values) {
						eval.append(v + "; ");
					}
					// Ask user for evaluation
					eval.append("\nEvaluating match " + ++matchIndex + "/"
							+ numMatches + "\n");
					eval.append("Does this value match '"
							+ formatQueryValue(queryKey)
							+ "' (+=yes -=no s=skip e=end table evaluation #=reject whole table x=exit)? ");

					char response = askCommand(eval.toString(), new char[] {
							'+', '-', 's', 'e', '#', 'x' });

					switch (response) {
					case '+':
						// correct match, keep for gold standard
						break;
					case '-':
						// incorrect match, try to find alternative match
						int falseId = e.getValue().getEntryID();
						IndexEntry newEntry = findAlternativeMatch(table,
								queryKey, falseId, indexLocation,
								r.getRightTable());

						if (newEntry != null) {
							// remove wrong match
							r.getJoinPairs().remove(e.getKey());
							// and add alternative, correct match
							r.getJoinPairs().put(e.getKey(), newEntry);
						} else {
							// no correct id can be found, remove key from gold
							// standard
							r.getJoinPairs().remove(e.getKey());
						}
						break;
					case 's':
						// skip this match = remove from gold standard
						r.getJoinPairs().remove(e.getKey());
						break;
					case 'e':
						// end table evaluation = skip all remaining matches
						r.getJoinPairs().remove(e.getKey());
						endTable = true;
						break;
					case 'x':
						// discard data about current table and exit application
						exit = true;
						break;
					case '#':
						// remove complete table from the gold standard
						rejectTable = true;
					default:

					} // switch(response)

					// Determine average time required per match
					double avg = ((double) (System.currentTimeMillis() - start) / (double) ++matchesEvaluated) / 1000.0;
					System.out.println("***** Current avg. time per match: "
							+ avg + "s *****");
				} // if(currentLine!=e.getValue().getEntryID()) else-part

				if (exit || rejectTable) {
					// add rejected table's name to the wrong tables list
					BufferedWriter w = new BufferedWriter(new FileWriter(
							wrongTablesFile, true));
					w.write(r.getRightTable() + "\n");
					w.close();
					break;
				}

				if (lst.size() == 0)
					break;
			} // for(Entry<IndexEntry, IndexEntry> e : entries)

			if (lst.size() != 0)
				; //reader.close();
			else
				// have to reject whole table, as it does not contain any new
				// matches
				rejectTable = true;

		} catch (Exception ex) {
			ex.printStackTrace();
			exit = true;
		}

		if (exit)
			return true;

		if (!rejectTable && r.getJoinPairs().size() > 0)
			goldStandard.add(r);

		rejectTable = false;

		double tblDuration = (double) (System.currentTimeMillis() - tableStart) / 1000.0;
		double avgTblMatch = ((double) (System.currentTimeMillis() - tableStart) / (double) matchesEvaluated) / 1000.0;
		System.out.println("********** Table completed in " + tblDuration
				+ "s, avg. time per match was " + avgTblMatch + "s **********");

		clearScreen();

		try {
			JoinResult.writeCsv(goldStandard, goldStandardFile);
		} catch (IOException e) {
			e.printStackTrace();
		}

		return false;
	}

	private IndexEntry getEntryForKey(String keyValue) {
		for (IndexEntry entry : queryTableEntries) {
			if (entry.getValue().equals(keyValue))
				return entry;
		}
		return null;
	}

	private List<Entry<IndexEntry, IndexEntry>> sortEntries(
			Set<Entry<IndexEntry, IndexEntry>> entries) {
		ArrayList<Entry<IndexEntry, IndexEntry>> result = new ArrayList<Map.Entry<IndexEntry, IndexEntry>>();

		for (Entry<IndexEntry, IndexEntry> e : entries) {
			result.add(e);
		}

		Collections.sort(result,
				new Comparator<Entry<IndexEntry, IndexEntry>>() {
					public int compare(Entry<IndexEntry, IndexEntry> o1,
							Entry<IndexEntry, IndexEntry> o2) {
						return Integer.compare(o1.getValue().getEntryID(), o2
								.getValue().getEntryID());
					};
				});

		return result;
	}

	private List<Entry<IndexEntry, IndexEntry>> sortEntries(
			List<Entry<IndexEntry, IndexEntry>> entries) {
		ArrayList<Entry<IndexEntry, IndexEntry>> result = new ArrayList<Map.Entry<IndexEntry, IndexEntry>>();

		for (Entry<IndexEntry, IndexEntry> e : entries) {
			result.add(e);
		}

		Collections.sort(result,
				new Comparator<Entry<IndexEntry, IndexEntry>>() {
					public int compare(Entry<IndexEntry, IndexEntry> o1,
							Entry<IndexEntry, IndexEntry> o2) {
						return Integer.compare(o1.getValue().getEntryID(), o2
								.getValue().getEntryID());
					};
				});

		return result;
	}

	private IndexEntry findAlternativeMatch(String tableFileName,
			IndexEntry key, int incorrectId, String indexLocation,
			String tableHeader) throws UnsupportedEncodingException,
			FileNotFoundException, IOException {

		// use index to determine alternative matches
		Pipeline p = new Pipeline("BuildGoldStandardPipeline", indexLocation);
		p.setKeyidentificationType(KeyIdentificationType.singleWithRefineAttrs);
		IndexManager im = new IndexManager(p);
		System.out.println("Searching alternatives for '"
				+ formatQueryValue(key) + "'");
		// List<IndexEntry> results = im.searchIndex(key);
		List<IndexEntry> results = im.searchIndexForGoldBuilder(key,
				tableHeader);
		// sort the entries by lucene score
		/*
		 * Collections.sort(results, new Comparator<IndexEntry>() { public int
		 * compare(IndexEntry e1, IndexEntry e2) { return
		 * -Float.compare(e1.getLuceneScore(), e2.getLuceneScore()); }; });
		 */

		TableReader r = null;

		// remove all results that are from a different table or already in the
		// gold standard
		Iterator<IndexEntry> resultIt = results.iterator();
		while (resultIt.hasNext()) {
			IndexEntry result = resultIt.next();

			String fileName = new File(tableFileName).getName();

			if (!result.getTabelHeader().equals(fileName) // the result is from
															// a different table
					|| result.getEntryID() == incorrectId // do not evaluate the
															// incorrect values
															// again
					|| !isNewMatch(key, result, true)) // do evaluate
														// values
														// already
														// in the
														// gold
														// standard
				resultIt.remove();
		}

		// if no alternatives for the same table can be found, cancel
		if (results.size() == 0)
			return null;

		int page = 0;
		int pageSize = 30;

		// loop through alternative matches
		while (results.size() > 0)
		// for(IndexEntry e : results)
		{
			page = 0;
			IndexEntry e = null;

			int numPages = results.size() / pageSize;
			if (results.size() % pageSize != 0)
				numPages++;

			Integer index = null;
			while (index == null) {
				// let user decide which alternative to evaluate next
				StringBuilder choices = new StringBuilder();
				choices.append("Choose an alternative to evaluate next for '"
						+ formatQueryValue(key) + "'\n");

				int cnt = 0;
				int pageStart, pageEnd;
				pageStart = page * pageSize;
				pageEnd = pageStart + pageSize - 1;
				for (IndexEntry entry : results) {
					if (cnt >= pageStart && cnt <= pageEnd) {
						choices.append("[" + (cnt + 1) + "]\t("
								+ ScoreEvaluator.get(p).assessIndexEntry(entry)
								+ ")\t" + entry.getTabelHeader() + "\t"
								+ entry.getColumnHeader() + "\t" + "(line "
								+ entry.getEntryID() + ")\t"
								+ formatQueryValue(entry) + "\n");
					}
					cnt++;
				}
				choices.append("Page " + (page + 1) + " of " + numPages + "\n");
				choices.append("\nEnter the number of the alternative to evaluate next (0 to skip, [return] for next page): ");
				index = askInt(choices.toString(), 0, results.size());

				if (index != null)
					index -= 1;

				if (index != null && index == -1)
					return null;

				if (++page == numPages)
					page = 0;
			}

			e = results.get(index);

			// Check TableReader
			if (r == null) {
				// no active reader, create one
				r = new TableReader(tableFileName);
				r.open();
			} else if (r.getCurrentLine() > e.getEntryID()) {
				// reader has advanced beyond the next entry, close and re-open
				r.close();
				r = new TableReader(tableFileName);
				r.open();
			}

			// load values from table
			String[] values = r.GetEntryValues(e.getEntryID());

			if (values == null) {
				// Id not contained in table, remove result
				results.remove(index);
				continue;
			}

			StringBuilder alt = new StringBuilder();
			alt.append("Alternative row for query key '"
					+ formatQueryValue(key) + "' ("
					+ ScoreEvaluator.get(p).assessIndexEntry(e) + "):\n");
			// print complete line of entity table
			for (String v : values) {
				alt.append(v + "; ");
			}
			// Ask user for evaluation
			alt.append("\nDoes this alternative match '"
					+ formatQueryValue(key) + "' (+=yes -=no s=skip key)? ");

			char response = askCommand(alt.toString(), new char[] { '+', '-',
					's' });

			switch (response) {
			case '+':
				// correct match, return id
				return e;
			case '-':
				// incorrect match, remove result and continue with next
				// alternative
				int i = index;
				results.remove(i);
				break;
			case 's':
				// skip this key, return original id
				return null;
			default:

			} // switch(response)
		}

		return null;
	}

	private char askCommand(String text, char[] commands) throws IOException {
		boolean handled = false;
		char response = '\0';

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));

		while (!handled) {
			System.out.print(text);

			String line = reader.readLine();

			if (line.trim().length() > 0)
				response = line.trim().charAt(0);
			// response = (char) System.in.read();

			// read the [return] key
			// System.in.read();

			for (char c : commands) {
				if (c == response)
					handled = true;
			}
		}

		return response;
	}

	private Integer askInt(String text, int min, int max) throws IOException {
		boolean handled = false;
		boolean hasError = false;
		int response = -1;

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));

		while (!handled) {
			System.out.print(text);

			if (hasError) {
				// for every \n output a new line
				for (char c : text.toCharArray())
					if (c == '\n')
						System.out.println();
				hasError = false;
			}

			String line = reader.readLine();

			if (line.isEmpty())
				return null;

			try {
				response = Integer.parseInt(line);

				if (response >= min && response <= max)
					handled = true;
				else if (response == -1) {
					// in case there is a problem with the output ...
					hasError = true;
					// for every \n output a new line
					for (char c : text.toCharArray())
						if (c == '\n')
							System.out.println("\n");
				}
			} catch (Exception ex) {
			}
		}

		return response;
	}

	private String formatQueryValue(IndexEntry key) {
		StringBuilder sb = new StringBuilder();

		sb.append(key.getValue());

		if (key.getRefineAttrs().size() > 0) {
			sb.append(" [");

			int cnt = 0;
			for (IndexEntry at : key.getRefineAttrs()) {
				if (cnt++ > 0)
					sb.append(", ");
				sb.append(at.getValue());
			}

			sb.append("]");
		}

		return sb.toString();
	}

}
