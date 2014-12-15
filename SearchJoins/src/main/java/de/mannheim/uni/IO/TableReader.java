package de.mannheim.uni.IO;

import java.io.File;

import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.datafusion.TableDataCleaner;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.CsvConverter;

public class TableReader {

	private Pipeline pipe;
	private ConvertFileToTable conv;
	
	public TableReader()
	{
		this(new Pipeline("default", ""));
	}
	
	public TableReader(Pipeline pipeline)
	{
		pipe = pipeline;
		conv = new ConvertFileToTable(pipe);
	}
	
	public Table readTable(String path)
	{
		//Table t = readWebTable(path);
		Table t = readQueryTable(path);
		
		//t = removeEmptyCols(t);
		
		//t = removeEmptyRows(t);
		
		return t;
	}
	
	public Table readTableFast(String path)
	{
		Table t = readQueryTableFast(path);
		
		return t;
	}
	
	public Table readTableFromString(String fileName, String content)
	{
		Table t = conv.readWebTableForIndexing(fileName, pipe.getInstanceStartIndex(), content);
		
		prepareTable(t);
		
		return t;
	}
	
	public Table readWebTable(String path)
	{
		path = convertTable(path);
		
		Table t = conv.readWebTableForIndexing(path, pipe.getInstanceStartIndex());
		
		prepareTable(t);
		
		return t;
	}
	
	public Table readQueryTable(String path)
	{
		path = convertTable(path);
		
		Table t = conv.readTableForSearch(path, pipe.getInstanceStartIndex());
		
		t.setSource(path);
		
		prepareTable(t);
		
		return t;
	}
	
	public Table readQueryTableFast(String path) {
		return readQueryTableFast(path, false);
	}
	
	public Table readQueryTableFast(String path, boolean keepLists)
	{
		path = convertTable(path, keepLists);
		
		Table t = conv.readTableForSearchFast(path, pipe.getInstanceStartIndex());
		
		t.setSource(path);
		
		prepareTable(t);
		
		return t;
	}
	
	public Table readLodTable(String path)
	{
		path = convertTable(path);
		
		Table t = conv.readLODtableForIndexing(path);
		
		prepareTable(t);
		
		return t;
	}
	
	private String convertTable(String path) {
		return convertTable(path, false);
	}
	
	private String convertTable(String path, boolean keepLists)
	{
		if(path.endsWith("csv"))
		{
			if(!new File(path + ".gz").exists())
			{
				CsvConverter c = new CsvConverter();
				c.convertCsv(path, path + ".gz", keepLists);
			}
			else
			{
				// if the archive is older than the csv, recreate archive
				if(new File(path + ".gz").lastModified()<new File(path).lastModified())
				{
					CsvConverter c = new CsvConverter();
					c.convertCsv(path, path + ".gz", keepLists);
				}
			}
			return path + ".gz";
		}
		else
			return path;
	}
	
	private void prepareTable(Table table)
	{
		if(table!=null)
		{
			// Set the final data type
			for(TableColumn c : table.getColumns())
			{
				c.setFinalDataType();
				c.setDataSource(table.getHeader());
			}
			
			// Identify the key
			TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(pipe);
			keyIdentifier.identifyKey(table);
		}
	}
	
	public Table removeEmptyRows(Table table)
	{
		TableDataCleaner c = new TableDataCleaner(table, pipe);
		
		return c.filterRowsByRowDensity(table);
	}
	
	public Table removeEmptyCols(Table table)
	{
		TableDataCleaner c = new TableDataCleaner(table, pipe);
		
		return c.filterColumnsByColumnDensity(table);
	}
}
