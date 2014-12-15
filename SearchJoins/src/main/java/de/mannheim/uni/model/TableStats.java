package de.mannheim.uni.model;

public class TableStats {

	private String header;

	private double nmCols;
	private double nmRows;
	private double nmNulls;

	private String language;

	private String timestamp;

	public String getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(String timestamp) {
		this.timestamp = timestamp;
	}

	public String getLanguage() {
		return language;
	}

	public void setLanguage(String language) {
		this.language = language;
	}

	private double avgNulls;

	public TableStats() {
		nmCols = nmNulls = nmRows = 0; // TODO Auto-generated constructor stub
	}

	public double getNmCols() {
		return nmCols;
	}

	public void setNmCols(double nmCols) {
		this.nmCols = nmCols;
	}

	public double getNmRows() {
		return nmRows;
	}

	public void setNmRows(double nmRows) {
		this.nmRows = nmRows;
	}

	public double getNmNulls() {
		return nmNulls;
	}

	public void setNmNulls(double nmNulls) {
		this.nmNulls = nmNulls;
	}

	public double getAvgNulls() {
		if (nmCols == 0.0 || nmRows == 0.0)
			return 0;

		return nmNulls / (nmCols * nmRows);
	}

	public void setAvgNulls(double avgNulls) {
		this.avgNulls = avgNulls;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public String getInfo() {
		return header + "|" + nmCols + "|" + nmRows + "|" + nmNulls + "|"
				+ getAvgNulls();
	}

}
