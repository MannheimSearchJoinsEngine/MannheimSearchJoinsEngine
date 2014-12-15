package de.mannheim.uni.model.schema;

import java.util.List;

import de.mannheim.uni.model.schema.ColumnMatch;

public class TableMatch {
	private String table1;
	private String table2;

	public String getTable1() {
		return table1;
	}

	public void setTable1(String table1) {
		this.table1 = table1;
	}

	public String getTable2() {
		return table2;
	}

	public void setTable2(String table2) {
		this.table2 = table2;
	}

	public List<ColumnMatch> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnMatch> columns) {
		this.columns = columns;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	private List<ColumnMatch> columns;
	private double score;
}
