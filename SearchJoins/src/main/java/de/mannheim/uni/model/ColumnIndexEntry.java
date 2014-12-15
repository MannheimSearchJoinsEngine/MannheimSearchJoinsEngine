package de.mannheim.uni.model;

import java.util.List;

public class ColumnIndexEntry {
	private int columnID;

	private String columnHeader;

	private String columnDataType;

	private int tableCardinality;

	private int columnDistinctValues;

	private String tableFullPath;

	// used for retrieveing
	private float luceneScore;

	private String tableHeader;

	/**
	 * this is the original value, unprocessed
	 * 
	 */
	private String columnOrignalHeader;

	public String getTableFullPath() {
		return tableFullPath;
	}

	public void setTableFullPath(String tableFullPath) {
		this.tableFullPath = tableFullPath;
	}

	public int getColumnID() {
		return columnID;
	}

	public void setColumnID(int columnID) {
		this.columnID = columnID;
	}

	public String getColumnHeader() {
		return columnHeader;
	}

	public void setColumnHeader(String columnHeader) {
		this.columnHeader = columnHeader;
	}

	public String getColumnDataType() {
		return columnDataType;
	}

	public void setColumnDataType(String columnDataType) {
		this.columnDataType = columnDataType;
	}

	public int getTableCardinality() {
		return tableCardinality;
	}

	public void setTableCardinality(int tableCardinality) {
		this.tableCardinality = tableCardinality;
	}

	public int getColumnDistinctValues() {
		return columnDistinctValues;
	}

	public void setColumnDistinctValues(int columnDistinctValues) {
		this.columnDistinctValues = columnDistinctValues;
	}

	public float getLuceneScore() {
		return luceneScore;
	}

	public void setLuceneScore(float luceneScore) {
		this.luceneScore = luceneScore;
	}

	public String getTableHeader() {
		return tableHeader;
	}

	public void setTableHeader(String tableHeader) {
		this.tableHeader = tableHeader;
	}

	public String getColumnOrignalHeader() {
		return columnOrignalHeader;
	}

	public void setColumnOrignalHeader(String columnOrignalHeader) {
		this.columnOrignalHeader = columnOrignalHeader;
	}

	public ColumnIndexEntry() {
		// TODO Auto-generated constructor stub
	}

}
