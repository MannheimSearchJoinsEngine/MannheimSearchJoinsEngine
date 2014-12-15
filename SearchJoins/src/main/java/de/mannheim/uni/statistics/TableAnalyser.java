package de.mannheim.uni.statistics;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.utils.PipelineConfig;

public class TableAnalyser {

	public void analyseTable(Table t, String outputDir)
	{
		writeTableReport(t, new File(outputDir, "table_report.txt").getAbsolutePath());
		writeColumnStatistics(t, new File(outputDir, "column_statistics.csv").getAbsolutePath());
	}
	
	private void writeTableReport(Table t, String output)
	{
		TableColumn key = t.getFirstKey();
		
		StringBuilder sb = new StringBuilder();
		
		sb.append("Table " + t.getHeader() + "\n");
		sb.append(t.getColumns().size() + " columns\n");
		
		if(key!=null)
		{
			sb.append(key.getValues().size() + " keys\n");
		}
		else
			sb.append("Table has no key!");
		
		try {
			FileUtils.writeStringToFile(new File(output), sb.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void writeColumnStatistics(Table t, String output)
	{
		StringBuilder sb = new StringBuilder();
		
		sb.append("header\tsource\ttype\tsize\tnum null values\n");
		
		for(TableColumn c : t.getColumns())
		{
			
			sb.append(c.getHeader());
			sb.append("\t");
			sb.append(c.getDataSource());
			sb.append("\t");
			sb.append(c.getDataType().toString());
			sb.append("\t");
			sb.append(c.getTotalSize());
			sb.append("\t");
			if(c.getValuesInfo().containsKey(PipelineConfig.NULL_VALUE))
				sb.append(c.getValuesInfo().get(PipelineConfig.NULL_VALUE));
			else
				sb.append("0");
			sb.append("\n");
		}
		
		try {
			FileUtils.writeStringToFile(new File(output), sb.toString());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
