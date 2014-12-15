package de.mannheim.uni.model.schema;

import java.util.ArrayList;
import java.util.List;

import de.mannheim.uni.model.TableColumn;

public class ColumnObjectsMatch {
	private TableColumn column1;
	private List<TableColumn> column2;
	private String columnType;
	private double score;
	private int instanceCount;

	public ColumnObjectsMatch()
	{
		column2 = new ArrayList<TableColumn>();
	}
	
	public String getColumnType() {
		return columnType;
	}

	public void setColumnType(String columnType) {
		this.columnType = columnType;
	}

	public TableColumn getColumn1() {
		return column1;
	}

	public void setColumn1(TableColumn column1) {
		this.column1 = column1;
	}

	public List<TableColumn> getColumn2() {
		return column2;
	}

	public void setColumn2(List<TableColumn> column2) {
		this.column2 = column2;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	public int getInstanceCount() {
		return instanceCount;
	}

	public void setInstanceCount(int instanceCount) {
		this.instanceCount = instanceCount;
	}

}
