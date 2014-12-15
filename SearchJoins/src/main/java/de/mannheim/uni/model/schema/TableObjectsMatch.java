package de.mannheim.uni.model.schema;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnMatch;

public class TableObjectsMatch {
	private String table1;
	private String table2;
	private List<ColumnObjectsMatch> columns;
	private double score;
	private List<ColumnObjectsMatch> columnPairs;

	public TableObjectsMatch()
	{
		columns = new LinkedList<ColumnObjectsMatch>();
	}
	
	public List<ColumnObjectsMatch> getColumnPairs() {
		return columnPairs;
	}

	public void setColumnPairs(List<ColumnObjectsMatch> columnPairs) {
		this.columnPairs = columnPairs;
	}

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

	public List<ColumnObjectsMatch> getColumns() {
		return columns;
	}

	public void setColumns(List<ColumnObjectsMatch> columns) {
		this.columns = columns;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public void producePairs() {
		columnPairs = new ArrayList<ColumnObjectsMatch>();
		for (ColumnObjectsMatch m : columns) {
			List<TableColumn> allColumns = m.getColumn2();
			allColumns.add(m.getColumn1());
			for (int i = 0; i < allColumns.size(); i++) {
				TableColumn first = allColumns.get(i);
				ColumnObjectsMatch newMatch = new ColumnObjectsMatch();
				for (int j = 0; j < allColumns.size(); j++) {
					TableColumn second = allColumns.get(j);
					if (first != second) {
						newMatch.setColumn1(first);
						newMatch.getColumn2().add(second);
						newMatch.setColumnType(m.getColumnType());
						newMatch.setScore(m.getScore());
						newMatch.setInstanceCount(m.getInstanceCount());
						columnPairs.add(newMatch);
					}
				}
			}
		}
	}
}
