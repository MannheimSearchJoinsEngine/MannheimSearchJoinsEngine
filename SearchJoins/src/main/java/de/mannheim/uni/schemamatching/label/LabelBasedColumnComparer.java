package de.mannheim.uni.schemamatching.label;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.pipelines.Pipeline;

/**
 * @author petar
 * 
 */
public class LabelBasedColumnComparer extends LabelBasedComparer implements
		Runnable {

	public LabelBasedColumnComparer(Pipeline pipeline, Table queryTable) {
		super(pipeline, queryTable, false);
		wordnetExactMatch = LabelBasedComparer.WORDNET_EXACT_MATCH_SCORE_DUPLICATES;
		dbpediaAndYagoExactMatch = LabelBasedComparer.WORDNET_EXACT_MATCH_SCORE_DUPLICATES;
		wordnetSynsetMatch = LabelBasedComparer.WORDNET_SYNSET_MATCH_SCORE_DUPLICATES;
		// TODO Auto-generated constructor stub
		results = null;
	}

	private TableColumn column1;
	private TableColumn column2;
	private ColumnScoreValue results;

	public ColumnScoreValue getResults() {
		return results;
	}

	protected void setResults(ColumnScoreValue results) {
		this.results = results;
	}

	public TableColumn getColumn1() {
		return column1;
	}

	public void setColumn1(TableColumn column1) {
		this.column1 = column1;
	}

	public TableColumn getColumn2() {
		return column2;
	}

	public void setColumn2(TableColumn column2) {
		this.column2 = column2;
	}

	public void run() {
		ColumnScoreValue result = compareColumns(column1, column2);
		this.queryTable = null;
		setResults(result);
	}

	public ColumnScoreValue compareColumns(TableColumn c1, TableColumn c2) {
		ColumnScoreValue sv = new ColumnScoreValue();

		sv.setType(c2.getDataType().toString());

		if (c1.getDataSource()!=null && c1.getDataSource().equals(c2.getDataSource())) {
			sv.Add(0, 1);
			return sv;
		}

		Double score = compareColumnValues(c1.getDataType().toString(),
				c1.getHeader(), c2.getDataType().toString(), c2.getHeader());

		sv.Add(score, 1);

		return sv;
	}

	private double compareColumnValues(String type1, String header,
			String type2, String header2) {
		double score = 0;

		ColumnDataType t;

		// two columns of different type are not at all similar
		if (!type1.equals(type2))
			return score;
		// System.out.println("LABEL BASED: Comparing " + header + ":" +
		// header2);
		long start = System.currentTimeMillis();
		score = matchStrings(header, header2);

		// System.out.println("LABEL BASED: Comparing " + header + ":" + header2
		// + " took" + (System.currentTimeMillis() - start));

		return score;
	}
}
