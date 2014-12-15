package de.mannheim.uni.model;

import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.utils.PipelineConfig;
import au.com.bytecode.opencsv.CSVWriter;

/**
 * @author petar
 * 
 */
public class Table {

	private boolean hasKey;

	private String fullPath;

	private String source;

	private List<TableColumn> columns;

	private String header;

	private List<TableColumn> compaundKeyColumns;

	public int nmNulls;

	public int getNmNulls() {
		return nmNulls;
	}

	public void setNmNulls(int nmNulls) {
		this.nmNulls = nmNulls;
	}

	public String getFullPath() {
		return fullPath;
	}

	public void setHasKey(boolean hasKey) {
		this.hasKey = hasKey;
	}

	public String getSource() {
		return source;
	}

	public void setFullPath(String fullPath) {
		this.fullPath = fullPath;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public List<TableColumn> getCompaundKeyColumns() {
		return compaundKeyColumns;
	}

	public void setCompaundKeyColumns(List<TableColumn> compaundKeyColumns) {
		this.compaundKeyColumns = compaundKeyColumns;
	}

	public List<TableColumn> getColumns() {
		return columns;
	}

	public void setColumns(List<TableColumn> columns) {
		this.columns = columns;
	}
        
        public void addColumn(TableColumn column) {
            this.columns.add(column);
        }
        
        public void deleteColumn(TableColumn column) {
            this.columns.remove(column);
        }

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		if (header.contains("\\"))
			header = header.substring(header.lastIndexOf("\\") + 1,
					header.length());
		if (header.contains("/"))
			header = header.substring(header.lastIndexOf("/") + 1,
					header.length());
		this.header = header;
	}

	public Table() {
		// TODO Auto-generated constructor stub
		columns = new LinkedList<TableColumn>();
		header = "";
		compaundKeyColumns = new ArrayList<TableColumn>();
		hasKey = true;
		nmNulls = 0;
	}

	public boolean isHasKey() {
		return hasKey;
	}

	public void printTable() {

	}
	
	public TableColumn getFirstKey()
	{
		for(TableColumn c : columns)
			if(c.isKey())
				return c;
		return null;
	}

	public void writeDetailedData(String fileName)
	{
		try {
			
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
					fileName), "UTF-8"));
			
			writer.writeNext(new String[] { 
					"Data Source",
					"Column Name",
					"Data Type",
					"Key",
					"Total Size",
					"Value"
			});
			
			String[] values = new String[6];
			
			for(TableColumn c : columns)
			{
				StringBuilder sb = new StringBuilder();
				
				values[0] = c.getDataSource();
				values[1] = c.getHeader();
				values[2] = c.getDataType().toString();
				values[3] = Boolean.toString(c.isKey());
				values[4] = Integer.toString(c.getTotalSize());
			
				for(String value : c.getValues().values())
				{
					values[5] = value;
					writer.writeNext(values);
				}
			}
			
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void writeTableToFile(String name) {
		writeTableToFile(name, true);
	}
	
	public void writeTableToFile(String name, boolean writeTypes) {
		if (name == null)
			name = header.replace(".gz", "");
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
					name + ".csv"), "UTF-8"));
		} catch (Exception e) {
			e.printStackTrace();
		}
		List<TableColumn> columnsOrdered = new LinkedList<TableColumn>();
		String[] columnHeaders = new String[columns.size()];
		String[] columnTypes = new String[columns.size()];

		// put the key columns first
		int index = 0;
		for (TableColumn col : columns) {
			if (col.isKey()) {
				columnsOrdered.add(col);
				columnHeaders[index] = col.getHeader();
				if (col.getDataSource() != null)
					columnHeaders[index] += "(" + col.getDataSource() + ")";
				if (col.getCleaningInfo() != null)
					columnHeaders[index] += " (" + col.getCleaningInfo() + ")";
				columnTypes[index] = col.getDataType().toString();
				index++;
			}
		}
		// add the rest of the columns
		for (TableColumn col : columns) {
			if (!col.isKey()) {
				columnsOrdered.add(col);
				columnHeaders[index] = col.getHeader();
				if (col.getDataSource() != null)
					columnHeaders[index] += "(" + col.getDataSource() + ")";
				if (col.getCleaningInfo() != null)
					columnHeaders[index] += " (" + col.getCleaningInfo() + ")";
				columnTypes[index] = col.getDataType().toString();
				if (col.getDataType() == ColumnDataType.unit
						&& col.getBaseUnit() != null)
					columnTypes[index] = col.getBaseUnit().getName();

				index++;
			}
		}

		// write the headers
		writer.writeNext(columnHeaders);
		// write the types
		if(writeTypes)
			writer.writeNext(columnTypes);

		//for (int i = 2; i < columnsOrdered.get(0).getValues().size() + 2; i++) {
		for(Integer i : columnsOrdered.get(0).getValues().keySet()) {
			String[] values = new String[columns.size()];
			int j = 0;
			for (TableColumn column : columnsOrdered) {
				// values[j] = PipelineConfig.NULL_VALUE;
				// if (column.getValues().containsKey(i))
				//values[j] = column.getValues().get(i);
				if(column.getValues().containsKey(i))
					values[j] = column.getValues().get(i);
				else
					values[j] = "";
				j++;
			}
			writer.writeNext(values);
		}

		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	
	public void writeColumnList(String FileName)
	{
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
					FileName), "UTF-8"));
			
			String[] values = new String[2];
			
			// write header
			values[0] = "Column name";
			values[1] = "Data Type";
			writer.writeNext(values);
			
			// write values
			for(TableColumn c : this.columns)
			{
				values[0] = c.getHeader();
				values[1] = c.getDataType().toString();
				writer.writeNext(values);
			}
			
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
