package de.mannheim.uni.statistics;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;

public class EvaluatedJoinResult
{
	private JoinResult result;

	public boolean isCorrect(IndexEntry e) {
		if(!correct.containsKey(e))
			return false;
		else
			return correct.get(e);
	}

	private HashMap<IndexEntry, Boolean> correct = new HashMap<IndexEntry, Boolean>();
	
	public void setCorrect(IndexEntry e, boolean isCorrect) {
		correct.put(e, isCorrect);
	}

	public JoinResult getResult() {
		return result;
	}

	public void setResult(JoinResult result) {
		this.result = result;
	}

	public static void writeDetailedCsv(List<EvaluatedJoinResult> results, String filename, boolean append)
			throws IOException {
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(
				new FileOutputStream(filename, append), "UTF-8"));

		String[] values = new String[28];

		int tableRank = 1;
		for (EvaluatedJoinResult er : results) {
			JoinResult r = er.getResult();
			int i = 0;
			
			// query table name
			values[i++] = r.getLeftTable();
			// query table key column
			values[i++] = r.getLeftColumn();
			// number of matches
			values[i++] = Integer.toString(r.getCount());

			// entity table name
			values[i++] = r.getRightTable();
			// entity table key column
			values[i++] = r.getRightColumn();
			// entity table row count
			values[i++] = Integer.toString(r.getRightColumncardinality());
			// entity table path
			String tblPath="";
			values[i] = "/home/pristosk/LOD2Tables/Yago2Tables/Output/";
			if (r.getJoinPairs().values().iterator().hasNext()
					&& r.getJoinPairs().values().iterator().next()
							.getFullTablePath() != null) {
				tblPath = r.getJoinPairs().values().iterator().next()
						.getFullTablePath();
				if (tblPath.startsWith("..")) {
					tblPath = tblPath.replace("../..", "/home/pristosk");
				}
			}
			values[i++] = tblPath;

			// values used for rank calculations ...			
			values[i++] = Integer.toString(r.getLeftColumnCardinality());

			values[i++] = Integer.toString(r.getRightColumncardinality());

			values[i++] = Integer.toString(r.getLeftColumnDistinctValues());

			values[i++] = Integer.toString(r.getRightColumnDistinctValues());

			values[i++] = Integer.toString(r.getCount());

			values[i++] = Integer.toString(r.getLeftSumMultiplicity());

			values[i++] = Integer.toString(r.getRightSumMultiplicity());

			values[i++] = Integer.toString(r.getJoinSize());
			
			// Lucene total rank
			values[i++] = Double.toString(r.getLuceneTotalRank()); 
			
			// Ranking-type-based rank
			values[i++] = Double.toString(r.getRank());
			
			// Label-based schema matching rank
			values[i++] = Double.toString(r.getLabelBasedSchemaMatchingRank());
			
			// total rank
			values[i++] = Double.toString(r.getTotalRank());
			
			// ranked position of this table in the results
			values[i] = Integer.toString(tableRank++);
			
			for (Entry<IndexEntry, IndexEntry> entry : r.getJoinPairs()
					.entrySet()) {
				int j=1;
				
				// Correct?
				values[i + j++] = Boolean.toString(er.isCorrect(entry.getValue()));
				
				// query table key value
				values[i + j++] = entry.getKey().getValue();

				// entity table key value
				values[i + j++] = entry.getValue().getValue();

				// entity table row number
				values[i + j++] = Integer.toString(entry.getValue().getEntryID());

				// number of refining attributes
				values[i + j++] = Integer.toString(entry.getValue().getRefineAttrs().size());
				
				// lucene score
				values[i + j++] = Double.toString(entry.getValue().getLuceneScore());
				
				// score added by refining attributes
				values[i + j++] = Double.toString(
						entry.getValue().getTotalScore()
						- entry.getValue().getLuceneScore());
				
				// total score
				values[i + j++] = Double.toString(entry.getValue().getTotalScore());
				
				writer.writeNext(values);
			}
		}

		writer.close();
	}
	
}
