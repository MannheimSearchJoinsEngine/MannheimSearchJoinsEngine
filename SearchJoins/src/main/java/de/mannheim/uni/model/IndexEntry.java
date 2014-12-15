package de.mannheim.uni.model;

import java.util.ArrayList;
import java.util.List;

/**
 * @author petar
 * 
 */
public class IndexEntry implements java.io.Serializable {

	private String tabelHeader;

	// if existing; for web tables from LOD we always have unique id, and for
	// other web tables we can simply use the index value in the table
	private int entryID;

	private String columnHeader;

	private String columnDataType;

	private String value;

	private int tableCardinality;

	private int columnDistinctValues;

	private int valueMultiplicity;

	// used for retrieveing
	private float luceneScore;

	private float totalScore;

	private String fullTablePath;

	private boolean isPrimaryKey;

	private int columnIndex;

	// refining attributes
	List<IndexEntry> refineAttrs;

	/**
	 * this is the original value, unprocessed
	 * 
	 */
	private String originalValue;

	public IndexEntry(String tabelHeader, int entryID, String columnHeader,
			String columnDataType, String value, int tableCardinality,
			int columnDistinctValues, int valueMultiplicity) {
		super();
		this.tabelHeader = tabelHeader;
		this.entryID = entryID;
		this.columnHeader = columnHeader;
		this.columnDataType = columnDataType;
		this.value = value;
		this.tableCardinality = tableCardinality;
		this.columnDistinctValues = columnDistinctValues;
		this.valueMultiplicity = valueMultiplicity;
		this.refineAttrs = new ArrayList<IndexEntry>();
		this.columnIndex = -1;
	}

	public void setColumnIndex(int columnIndex) {
		this.columnIndex = columnIndex;
	}

	public int getColumnIndex() {
		return columnIndex;
	}

	public void setRefineAttrs(List<IndexEntry> refineAttrs) {
		this.refineAttrs = refineAttrs;
	}

	public List<IndexEntry> getRefineAttrs() {
		return refineAttrs;
	}

	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public String getFullTablePath() {
		return fullTablePath;
	}

	public void setFullTablePath(String fullTablePath) {
		this.fullTablePath = fullTablePath;
	}

	public IndexEntry() {
		this.isPrimaryKey = false;
		this.refineAttrs = new ArrayList<IndexEntry>();
		// TODO Auto-generated constructor stub
	}

	public String getTabelHeader() {
		return tabelHeader;
	}

	public void setTabelHeader(String tabelHeader) {
		this.tabelHeader = tabelHeader;
	}

	public int getEntryID() {
		return entryID;
	}

	public void setEntryID(int entryID) {
		this.entryID = entryID;
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

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
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

	public int getValueMultiplicity() {
		return valueMultiplicity;
	}

	public void setValueMultiplicity(int valueMultiplicity) {
		this.valueMultiplicity = valueMultiplicity;
	}

	public float getLuceneScore() {
		return luceneScore;
	}

	public void setLuceneScore(float luceneScore) {
		this.luceneScore = luceneScore;
	}

	public String getOriginalValue() {
		return originalValue;
	}

	public void setOriginalValue(String originalValue) {
		this.originalValue = originalValue;
	}

	public float getTotalScore() {
		return totalScore;
	}

	public void setTotalScore(float totalScore) {
		this.totalScore = totalScore;
	}

	public void printEntry() {
		System.out.println(tabelHeader + "|" + columnHeader + "|"
				+ columnDataType + "|" + tableCardinality + "|"
				+ columnDistinctValues + "|" + valueMultiplicity + "|" + value
				+ "|" + luceneScore);
	}

}
