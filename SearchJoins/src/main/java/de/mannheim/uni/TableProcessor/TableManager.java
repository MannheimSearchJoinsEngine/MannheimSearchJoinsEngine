package de.mannheim.uni.TableProcessor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class TableManager {

	/**
	 * returns the columns of the table for indexing
	 * 
	 * @param table
	 * @return
	 */
	public static List<ColumnIndexEntry> getColumnsEntriesFromTable(Table table) {
		List<ColumnIndexEntry> indexEntries = new ArrayList<ColumnIndexEntry>();
		int i = 0;
		for (TableColumn column : table.getColumns()) {
			ColumnIndexEntry entry = new ColumnIndexEntry();
			entry.setColumnDataType(column.getDataType().toString());
			entry.setColumnDistinctValues(column.getValuesInfo().size());
			entry.setTableHeader(table.getHeader());
			entry.setTableFullPath(table.getFullPath());
			entry.setTableCardinality(column.getTotalSize());
			entry.setColumnHeader(StringNormalizer.clearString(
					column.getHeader(), true));
			entry.setColumnOrignalHeader(column.getHeader());
			entry.setColumnID(i);
			indexEntries.add(entry);
			i++;
		}
		return indexEntries;
	}

	public static List<IndexEntry> getIndexEntriesFromTable(Table table) {
		List<IndexEntry> indexEntries = new ArrayList<IndexEntry>();
		for (TableColumn column : table.getColumns()) {

			// add the null values
			if (column.getValuesInfo().containsKey(
					PipelineConfig.NULL_VALUE))
				table.setNmNulls(table.getNmNulls()
						+ column.getValuesInfo().get(
								PipelineConfig.NULL_VALUE));
			if (column.getValuesInfo().containsKey(PipelineConfig.NULL_VALUE))
				table.setNmNulls(table.getNmNulls()
						+ column.getValuesInfo().get(PipelineConfig.NULL_VALUE));

			// get entries for column
			indexEntries.addAll(getEntriesForColumn(column, table,
					KeyIdentificationType.none));

		}
		return indexEntries;
	}

	public static List<IndexEntry> getKeyIndexEntriesFromTable(Table table) {
		List<IndexEntry> indexEntries = new LinkedList<IndexEntry>();
		for (TableColumn column : table.getColumns()) {
			if (column.isKey())
				indexEntries.addAll(getEntriesForColumn(column, table,
						KeyIdentificationType.single));

		}
		return indexEntries;
	}

	public static List<IndexEntry> getEntriesForColumn(TableColumn column,
			Table table, KeyIdentificationType keyType) {
		List<IndexEntry> indexEntries = new LinkedList<IndexEntry>();

		for (Entry<Integer, String> valueInfo : column.getValues().entrySet()) {
			int rowNM = valueInfo.getKey();// column.getIndexesByValue().get(valueInfo.getValue());

			IndexEntry entry = createIndexEntryForValue(rowNM, column, table);

			if (column.isKey()) {
				entry.setPrimaryKey(true);
				if (keyType == KeyIdentificationType.singleWithRefineAttrs) {
					List<IndexEntry> refineAtts = new ArrayList<IndexEntry>();
					for (TableColumn columnRefine : table.getColumns()) {
						if (columnRefine.getHeader().equals(column.getHeader()))
							continue;
						refineAtts.add(createIndexEntryForValue(rowNM,
								columnRefine, table));

					}
					entry.setRefineAttrs(refineAtts);

				}
			} else
				entry.setPrimaryKey(false);
			indexEntries.add(entry);
		}
		return indexEntries;
	}

	private static IndexEntry createIndexEntryForValue(int rowNM,
			TableColumn columnRefine, Table table) {

		IndexEntry entry = new IndexEntry();
		String vaule = columnRefine.getValues().get(rowNM);
		entry.setEntryID(rowNM);
		entry.setColumnDataType(columnRefine.getDataType().toString());
		entry.setColumnHeader(columnRefine.getHeader());
		entry.setColumnDistinctValues(columnRefine.getValuesInfo().size());
		entry.setTabelHeader(table.getHeader());
		entry.setTableCardinality(columnRefine.getTotalSize());
		entry.setOriginalValue(vaule);
		entry.setValueMultiplicity(columnRefine.getValuesInfo().get(vaule));
		// if it is not a string we don't want to normalize the value
		if (entry.getColumnDataType() == "string"
				|| entry.getColumnDataType() == "list") {
			vaule = StringNormalizer.clearString(vaule, true);
		} else {
			vaule = ConvertFileToTable.simpleStringNormalization(vaule, true);
		}

		entry.setValue(vaule);
		entry.setFullTablePath(table.getFullPath());
		return entry;

	}

	public static void removeNonStringColumns(Table table) {

		// remove non string columns
		List<Integer> toRemovecolumns = new ArrayList<Integer>();
		int i = 0;
		for (TableColumn column : table.getColumns()) {
			if (column.getDataType() != ColumnDataType.string)
				// && column.getDataType() != ColumnDataType.link)
				toRemovecolumns.add(i);
			i++;
		}

		i = 0;
		for (Integer j : toRemovecolumns) {
			table.getColumns().remove(j - i);
			i++;
		}
	}
}
