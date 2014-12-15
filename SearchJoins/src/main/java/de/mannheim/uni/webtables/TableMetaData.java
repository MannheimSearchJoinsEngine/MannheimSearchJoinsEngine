package de.mannheim.uni.webtables;

public class TableMetaData {

	private String url;
	private String tld;
	private String file;
	private int numCols;
	private int numRows;
	private int tableStart;
	private int tableEnd;
	
	public static TableMetaData fromStringArray(String[] values)
	{
		TableMetaData tmd = new TableMetaData();
		tmd.setUrl(values[0]);
		tmd.setTld(values[1]);
		tmd.setFile(values[2]);
		tmd.setNumCols(Integer.parseInt(values[3]));
		tmd.setNumRows(Integer.parseInt(values[4]));
		tmd.setTableStart(Integer.parseInt(values[5]));
		tmd.setTableEnd(Integer.parseInt(values[6]));
		
		return tmd;
	}
	
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getTld() {
		return tld;
	}
	public void setTld(String tld) {
		this.tld = tld;
	}
	public String getFile() {
		return file;
	}
	public void setFile(String file) {
		this.file = file;
	}
	public int getNumCols() {
		return numCols;
	}
	public void setNumCols(int numCols) {
		this.numCols = numCols;
	}
	public int getNumRows() {
		return numRows;
	}
	public void setNumRows(int numRows) {
		this.numRows = numRows;
	}
	public int getTableStart() {
		return tableStart;
	}
	public void setTableStart(int tableStart) {
		this.tableStart = tableStart;
	}
	public int getTableEnd() {
		return tableEnd;
	}
	public void setTableEnd(int tableEnd) {
		this.tableEnd = tableEnd;
	}
	
	
}
