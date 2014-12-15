package de.mannheim.uni.mse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.datafusion.DataFuser;
import de.mannheim.uni.index.TableInMemoryIndex;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.scoring.ScoreEvaluator;
import de.mannheim.uni.utils.PipelineConfig;

public class Join {

	private Pipeline pipe;
	
	public Join()
	{
		this(new Pipeline("default", ""));
	}
	
	public Join(Pipeline pipeline)
	{
		pipe = pipeline;
	}
	
	public Table joinTables(Table left, Table right)
	{
		DataFuser df = new DataFuser(pipe, new TableInMemoryIndex(right));
		
		Map<String, JoinResult> joinResult = table2JoinResult(left.getFirstKey(), left, right, null);
		
		System.out.println("JoinResults");
		for(String s : joinResult.keySet())
		{
			System.out.println(s);
			JoinResult jr = joinResult.get(s);
			System.out.println(jr.getLeftColumn() + " -> " + jr.getRightColumn());
			
			for(Entry<IndexEntry, IndexEntry> e : jr.getJoinPairs().entrySet())
			{
				System.out.println(e.getKey().getColumnHeader() + ": " + e.getKey().getValue() + " -> " + e.getValue().getColumnHeader() + ": " + e.getValue().getValue());
			}
		}
		
		Table result = df.fuseCompleteTable(left, new ArrayList<JoinResult>(joinResult.values()), null);
		
		return result;
	}
	
	/*
	 * Based on de.mannheim.uni.searchjoin.SearchJoin.findJoinsForColumn.
	 * Converts a right table to a JoinResult so it can be joined using de.mannheim.uni.datafusion.DataFuser
	 */
	public Map<String, JoinResult> table2JoinResult(TableColumn column,
			Table table, Table rightTable, Map<String, Map<Integer, Double>> tsp) {		
		
		Map<String, JoinResult> columnJoinResults = new HashMap<String, JoinResult>();
		
		List<IndexEntry> indexEntries = TableManager.getKeyIndexEntriesFromTable(table);
		
		for (IndexEntry leftEntry : indexEntries) {
			// if the value is null we are not interested in the results
			if (leftEntry.getValue().equalsIgnoreCase(PipelineConfig.NULL_VALUE))
				continue;
			List<IndexEntry> rightEntries = null;
			// use the standard index or the infoGather
			if (tsp == null)
				rightEntries = getIndexEntriesForKey(leftEntry, rightTable);
			else {
				// retrieve all entries
				rightEntries = getIndexEntriesForKey(leftEntry, rightTable);
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
				entryScore = ScoreEvaluator.get(pipe).assessIndexEntry(
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
					tmpScore = ScoreEvaluator.get(pipe).assessIndexEntry(
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

	public List<IndexEntry> getIndexEntriesForKey(IndexEntry key, Table table)
	{
		List<IndexEntry> entries = new LinkedList<IndexEntry>();
		
		List<IndexEntry> rightEntries = TableManager.getKeyIndexEntriesFromTable(table);
		
		for(IndexEntry e : rightEntries)
		{
			if(e.getValue().equals(key.getValue()))
					entries.add(e);
		}
		/*for(IndexEntry right : rightEntries)
		{
			if(right.getValue().equals(key.getValue()))
			{
				for(TableColumn c : table.getColumns())
				{
					if(!c.isKey())
					{
						List<IndexEntry> colValues = TableManager.getEntriesForColumn(c, table, KeyIdentificationType.singleWithRefineAttrs);
						
						entries.addAll(colValues);
					}
				}
			}
		}*/
		
		return entries;
	}
	
}
