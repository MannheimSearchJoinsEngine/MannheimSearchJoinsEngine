package de.mannheim.uni.schemamatching.instance;

import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;

import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableMatch;
import de.mannheim.uni.pipelines.Pipeline;

public class InstanceBasedResultComparer extends InstanceBasedComparer {
	private List<Entry<IndexEntry, IndexEntry>> table1;
	private List<Entry<IndexEntry, IndexEntry>> table2;
	private Pipeline pipeline;
	private TableMatch result;

	public TableMatch getResult() {
		return result;
	}

	protected void setResults(TableMatch result) {
		this.result = result;
	}

	public List<Entry<IndexEntry, IndexEntry>> getTable1() {
		return table1;
	}

	public void setTable1(List<Entry<IndexEntry, IndexEntry>> table1) {
		this.table1 = table1;
	}

	public List<Entry<IndexEntry, IndexEntry>> getTable2() {
		return table2;
	}

	public void setTable2(List<Entry<IndexEntry, IndexEntry>> table2) {
		this.table2 = table2;
	}

	public Pipeline getPipeline() {
		return pipeline;
	}

	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	public void run() {
		TableMatch result = compareTablesFromIndex(table1, table2, pipeline);
		setResults(result);
	}

	private TableMatch compareTablesFromIndex(
			List<Entry<IndexEntry, IndexEntry>> t1,
			List<Entry<IndexEntry, IndexEntry>> t2, Pipeline p) {
		IndexManager im = p.getIndexManager();

		HashMap<String, HashMap<String, ColumnScoreValue>> scores = new HashMap<String, HashMap<String, ColumnScoreValue>>();

		String table1 = null, table2 = null;
		Integer colCnt1 = null, colCnt2 = null;

		// for all pairs of rows describing the same entity
		for (int i = 0; i < t1.size(); i++) // all rows from table 1
		{
			IndexEntry t1Key = t1.get(i).getKey();
			IndexEntry t1Value = t1.get(i).getValue();

			if (table1 == null)
				table1 = t1Value.getTabelHeader();

			// System.out.println("Key = " + t1Key.getValue());

			for (int j = 0; j < t2.size(); j++) // all rows from table 2
			{
				IndexEntry t2Key = t2.get(j).getKey();
				IndexEntry t2Value = t2.get(j).getValue();

				if (table2 == null) {
					table2 = t2Value.getTabelHeader();
					// System.out.println("\nComparing tables " + table1 +
					// " and " + table2);
				}

				if (t1Key.getValue().equals(t2Key.getValue())) // Both rows
																// refer to the
																// same key
				{
					// get all values of both rows from index
					List<IndexEntry> row1 = im.getRowValues(
							t1Value.getTabelHeader(), t1Value.getEntryID());
					List<IndexEntry> row2 = im.getRowValues(
							t2Value.getTabelHeader(), t2Value.getEntryID());

					if (colCnt1 == null)
						colCnt1 = row1.size();
					else if (row1.size() > colCnt1)
						colCnt1 = row1.size();

					if (colCnt2 == null)
						colCnt2 = row2.size();
					else if (row2.size() > colCnt2)
						colCnt2 = row2.size();

					// for all pairs of columns that are no key
					for (IndexEntry col1 : row1)
						// all columns from table 1
						if (!col1.isPrimaryKey()) {
							if (!scores.containsKey(col1.getColumnHeader()))
								scores.put(col1.getColumnHeader(),
										new HashMap<String, ColumnScoreValue>());

							for (IndexEntry col2 : row2)
								// all columns from table 2
								if (!col2.isPrimaryKey()) {
									HashMap<String, ColumnScoreValue> score = scores
											.get(col1.getColumnHeader());

									if (!score.containsKey(col2
											.getColumnHeader()))
										score.put(col2.getColumnHeader(),
												new ColumnScoreValue());

									ColumnScoreValue sv = score.get(col2
											.getColumnHeader());

									if (sv.getType() == null)
										sv.setType(col2.getColumnDataType());

									// compare column values
									//TODO we cannot efficiently compute min and max values here, should be done in preprocessing ...
									sv.Add(compareIndexEntries(col1, col2, new MinMax()),i);
								}
						}
				}
			}
		}

		return decideMatching(scores, table1, table2);
	}

	protected double compareIndexEntries(IndexEntry e1, IndexEntry e2, MinMax minMax) {
		return compareColumnValues(e1.getColumnDataType(), e1.getValue(),
				e1.getColumnHeader(), e2.getColumnDataType(), e2.getValue(),
				e2.getColumnHeader(), minMax);
	}
}
