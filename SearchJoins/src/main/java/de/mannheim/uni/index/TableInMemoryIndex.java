package de.mannheim.uni.index;

import java.util.ArrayList;
import java.util.List;

import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;

public class TableInMemoryIndex implements IValueIndex {

	Table table;
	
	public TableInMemoryIndex(Table table)
	{
		this.table = table;
	}
	
	public boolean hasHeader(String tableName, List<String> allowedHeaders)
	{
		return true;
	}
	
	public List<IndexEntry> getRowValues(String tableName, int rowNm) {
		return getRowValues(tableName, rowNm, null);
	}
	
	public List<IndexEntry> getRowValues(String tableName, int rowNm, List<String> allowedHeaders) {
		List<IndexEntry> result = new ArrayList<IndexEntry>(table.getColumns().size());
		
		for(TableColumn c : table.getColumns())
		{
			if(c.getValues().containsKey((Integer)rowNm))
			{
				IndexEntry entry = new IndexEntry();
				entry.setLuceneScore(1);
				entry.setEntryID(1);
				entry.setColumnDataType(c.getDataType().toString());
				entry.setColumnHeader(c.getHeader());
				entry.setColumnDistinctValues(c.computeUniqueValues());
				entry.setTabelHeader(tableName);
				
				entry.setTableCardinality(c.getTotalSize());
				entry.setValueMultiplicity(c.getValuesInfo().get(c.getValues().get((Integer)rowNm)));
				entry.setValue(c.getValues().get((Integer)rowNm));
				entry.setFullTablePath(table.getFullPath());
				entry.setOriginalValue(c.getValues().get((Integer)rowNm));
				
				if (c.isKey()) {
					entry.setPrimaryKey(true);
				} else
					entry.setPrimaryKey(false);
				result.add(entry);
			}
		}
		
		return result;
	}

}
