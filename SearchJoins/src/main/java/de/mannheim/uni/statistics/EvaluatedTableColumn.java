package de.mannheim.uni.statistics;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;

public class EvaluatedTableColumn {

	private TableColumn tableColumn;
	private Boolean isCorrect;
	
	public TableColumn getTableColumn() {
		return tableColumn;
	}
	public void setTableColumn(TableColumn tableColumn) {
		this.tableColumn = tableColumn;
	}
	public Boolean getIsCorrect() {
		return isCorrect;
	}
	public void setIsCorrect(Boolean isCorrect) {
		this.isCorrect = isCorrect;
	}
	
	public List<EvaluatedTableColumn> readColumns(String fileName)
	{
		CSVReader r = null;

		List<EvaluatedTableColumn> results = new ArrayList<EvaluatedTableColumn>();
		
		try
		{
			r = new CSVReader(new InputStreamReader(new FileInputStream(fileName), "UTF-8"));
			
			for(String[] values : r.readAll())
			{
				EvaluatedTableColumn ec = new EvaluatedTableColumn();
				TableColumn c = new TableColumn();
				
				ec.setIsCorrect(Boolean.parseBoolean(values[0]));
				
				c.setHeader(values[1]);
				c.setDataType(ColumnDataType.valueOf(values[2]));
				
				ec.setTableColumn(c);
				
				results.add(ec);
			}
			
			r.close();
			
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return results;
	}
	
	public void writeColumns(String fileName, List<EvaluatedTableColumn> columns)
	{
		CSVWriter writer = null;
		try {
			writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
					fileName), "UTF-8"));
			
			String[] values = new String[2];
			
			// write header
			values[0] = "Correct";
			values[1] = "Column name";
			values[2] = "Data Type";
			
			// write values
			for(EvaluatedTableColumn c : columns)
			{
				values[0] = c.getIsCorrect().toString();
				values[1] = c.getTableColumn().getHeader();
				values[2] = c.getTableColumn().getDataType().toString();
				writer.writeNext(values);
			}
			
			writer.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
