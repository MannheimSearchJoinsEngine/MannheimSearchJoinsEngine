package de.mannheim.uni.TableProcessor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.ValueComparator;

/**
 * @author petar
 * 
 */
/**
 * detects keys the code for compaund key is commented out, because it is too
 * complex and cannot work on big tables
 * 
 * @author petar
 * 
 */
public class TableKeyIdentifier {

	private Pipeline pipeline;

	public TableKeyIdentifier(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	public Map<String, Double> identifyKey(Table table) {
		Map<String, Double> columnUniqueness = new HashMap<String, Double>();

		switch (pipeline.getKeyidentificationType()) {
		case none:
			columnUniqueness = null;
			break;

		case single:
			columnUniqueness = identifyKeysNaive(table,
					pipeline.isRemoveNonStrings());
			break;
		case singleWithRefineAttrs:
			columnUniqueness = identifyKeysNaive(table,
					pipeline.isRemoveNonStrings());
			break;

		case compaund:
			// identifyCompaundKey(table);
			break;

		}
		return columnUniqueness;
	}

	private Map<String, Double> identifyKeysNaive(Table table,
			boolean removeNonStrings) {
		List<TableColumn> keys = new ArrayList<TableColumn>();
		Map<String, Double> columnUniqueness = new LinkedHashMap<String, Double>();
		// check if there is a common key
		boolean isKeySet = false;
		for (TableColumn column : table.getColumns()) {
			if (column.getHeader().contains("#label")) {
				column.setKey(true);
				isKeySet = true;
				keys.add(column);
				break;
			}
		}
		if (!isKeySet) {

			for (TableColumn column : table.getColumns()) {
				if (column.getHeader().contains("#name")
						|| column.getHeader().contains("name")
						|| column.getHeader().contains("label")) {
					if (column.getHeader().contains("_label"))
						continue;
					column.setKey(true);
					isKeySet = true;
					keys.add(column);
					break;
				}
			}
		}
		long start = System.currentTimeMillis();

		List<TableColumn> keyColumns = new ArrayList<TableColumn>();
		// remove non string columns
		if (removeNonStrings)
			TableManager.removeNonStringColumns(table);

		int i = 0;
		for (TableColumn column : table.getColumns()) {
			if (column.getAverageValuesSize() > pipeline
					.getAverageKeyValuesLimitMin()
					&& column.getAverageValuesSize() <= pipeline
							.getAverageKeyValuesLimitMax()) {
				if (pipeline.getKeyidentificationType() == KeyIdentificationType.singleWithRefineAttrs)

				{
					if (column.getDataType() == TableColumn.ColumnDataType.string)
						columnUniqueness.put(Integer.toString(i),
								column.getColumnUniqnessRank());
				} else {
					columnUniqueness.put(Integer.toString(i),
							column.getColumnUniqnessRank());
				}

			}
			i++;
		}
		if (columnUniqueness.size() == 0)
			return null;
		double maxCount = columnUniqueness.values().iterator().next();
		String maxColumn = columnUniqueness.keySet().iterator().next();
		for (Entry<String, Double> entry : columnUniqueness.entrySet()) {
			if (entry.getValue() > maxCount) {
				maxCount = (Double) entry.getValue();
				maxColumn = entry.getKey();
			}
		}

		if (!isKeySet)
			table.getColumns().get(Integer.parseInt(maxColumn)).setKey(true);

		if (!isKeySet)
			keys.add(table.getColumns().get(Integer.parseInt(maxColumn)));
		if (keys.size() > 0) {
			table.setCompaundKeyColumns(keys);
		} else {
			table.setHasKey(false);
		}

		// no key was found
		if (maxCount < pipeline.getKeyUniqueness()) {
			if (!pipeline.isSilent())
				pipeline.getLogger()
						.info("The table"
								+ table.getFullPath()
								+ " doesn't have a Key! decrease the key uniqueness limit to find another key!");
			table.setHasKey(false);
			return columnUniqueness;
		}

		if (!pipeline.isSilent()) {
			pipeline.getLogger().info(
					"The key column is " + keys.get(0).getHeader()
							+ " with uniqueness of " + maxCount);

			long end = System.currentTimeMillis();
			pipeline.getLogger().info(
					"Time for single key identification: "
							+ ((double) (end - start) / 1000));
		}
		return columnUniqueness;
	}

	private boolean checkIfKey(String colId, Table table) {
		TableColumn column = table.getColumns().get(Integer.parseInt(colId));
		if (column.getColumnUniqnessRank() >= pipeline.getKeyUniqueness())
			return true;
		double frac = 0.0;
		if (column.getValuesInfo().containsKey(PipelineConfig.NULL_VALUE))
			frac = ((double) (column.getValuesInfo().get(
					PipelineConfig.NULL_VALUE) / (double) column.getTotalSize()));
		if (column.getColumnUniqnessRankWoNull() >= pipeline.getKeyUniqueness()
				&& frac <= pipeline.getKeyNullValuesFraction())
			return true;
		return false;
	}

	// private void identifyCompaundKey(Table table) {
	// Map<String, Double> columnUniqueness = identifyKeysNaive(table, false);
	// Map<String, Double> sortedColumns = sortMap(columnUniqueness);
	// if (checkIfKey(sortedColumns.keySet().iterator().next(), table))
	// return;
	//
	// // final key columns
	// List<TableColumn> keyColumns = new ArrayList<TableColumn>();
	//
	// Map<String, Map<String, List<Integer>>> generatedPairs = new
	// HashMap<String, Map<String, List<Integer>>>();
	//
	// // create pairs of keys
	// for (Entry<String, Double> firstEntry : sortedColumns.entrySet()) {
	// int firstColumnIndex = Integer.parseInt(firstEntry.getKey());
	// TableColumn firstcolumn = table.getColumns().get(firstColumnIndex);
	// // count the nm of unique values
	//
	// int uniqeValuesOld = table.getColumns().get(0).getTotalSize();
	// for (Entry<String, List<Integer>> entry : firstcolumn
	// .getNonUniqueValues().entrySet()) {
	// if (entry.getValue().size() > 1)
	// uniqeValuesOld -= entry.getValue().size();
	// }
	//
	// // the second entry
	// for (Entry<String, Double> second : sortedColumns.entrySet()) {
	// int uniqeValues = uniqeValuesOld;
	// int secondColumnIndex = Integer.parseInt(second.getKey());
	// // if it is the same as the first, skip it
	// if (firstColumnIndex == secondColumnIndex)
	// continue;
	//
	// TableColumn secondColumn = table.getColumns().get(
	// secondColumnIndex);
	// // used to remove the entires that are uniuqe
	// List<String> toRemoveEntries = new ArrayList<String>();
	// Map<String, List<Integer>> notResolved = new HashMap<String,
	// List<Integer>>();
	//
	// // go through all values
	// for (Entry<String, List<Integer>> nonUniqueEntry : firstcolumn
	// .getNonUniqueValues().entrySet()) {
	// List<Integer> newNotUniquesIndexes = new ArrayList<Integer>();
	// List<Integer> nonUniqueIndexes = nonUniqueEntry.getValue();
	// String cellValue = nonUniqueEntry.getKey();
	// // if the value is unique skip it and remove it for further
	// // use
	// // TODO: take care of the null values
	// if (nonUniqueIndexes.size() == 1
	// || cellValue.equals(PipelineConfig.NULL_VALUE
	// .toLowerCase())) {
	// toRemoveEntries.add(cellValue);
	// continue;
	// }
	// // put the unique values from the second column
	// List<String> tmpUniqueValues = new ArrayList<String>();
	// boolean isUnique = true;
	//
	// Map<String, Integer> passedValues = new HashMap<String, Integer>();
	// for (Integer indexFromSecondColumn : nonUniqueIndexes) {
	// String valueTocheck = secondColumn.getValues().get(
	// indexFromSecondColumn);
	// // if already exists then it is not unique
	// if (tmpUniqueValues.contains(valueTocheck)) {
	// isUnique = false;
	// newNotUniquesIndexes.add(indexFromSecondColumn);
	// if (passedValues.containsKey(valueTocheck))
	// if (!newNotUniquesIndexes.contains(passedValues
	// .get(valueTocheck)))
	// newNotUniquesIndexes.add(passedValues
	// .get(valueTocheck));
	// } else {
	// passedValues.put(valueTocheck,
	// indexFromSecondColumn);
	// }
	// tmpUniqueValues.add(valueTocheck);
	// }
	//
	// uniqeValues += (nonUniqueIndexes.size() - newNotUniquesIndexes
	// .size());
	// if (!isUnique)
	// notResolved.put(nonUniqueEntry.getKey(),
	// newNotUniquesIndexes);
	// }
	// // remove the unique strings to not be checked again
	// for (String string : toRemoveEntries)
	// firstcolumn.getNonUniqueValues().remove(string);
	// // set the new key if the uniqueness is greater than ..
	// double rank = (double) ((double) uniqeValues / (double) firstcolumn
	// .getTotalSize());
	// boolean isKey = false;
	// if (rank >= pipeline.getKeyUniqueness())
	// isKey = true;
	// if (firstcolumn.getValuesInfo().containsKey(
	// PipelineConfig.NULL_VALUE.toLowerCase())) {
	// rank = (double) ((double) uniqeValues / (double) ((double) firstcolumn
	// .getTotalSize() - firstcolumn.getValuesInfo().get(
	// PipelineConfig.NULL_VALUE.toLowerCase())));
	// double frac = ((double) (firstcolumn.getValuesInfo().get(
	// PipelineConfig.NULL_VALUE.toLowerCase()) / (double) firstcolumn
	// .getTotalSize()));
	// if (rank >= pipeline.getKeyUniqueness()
	// && frac <= pipeline.getKeyNullValuesFraction())
	// isKey = true;
	// }
	// if (isKey) {
	// firstcolumn.setKey(true);
	// secondColumn.setKey(true);
	//
	// keyColumns.add(firstcolumn);
	// keyColumns.add(secondColumn);
	//
	// break;
	// }
	//
	// generatedPairs.put(firstEntry.getKey() + "|" + second.getKey(),
	// notResolved);
	// }
	// if (keyColumns.size() > 0)
	// break;
	//
	// }
	// if (keyColumns.size() == 0)
	// getCompundKeysRecursive(generatedPairs, keyColumns, table,
	// sortedColumns);
	// if (keyColumns.size() > 0)
	// setKeyColumns(keyColumns, table);
	// else
	// pipeline.getLogger().info("There is no key");
	// }
	//
	// private void getCompundKeysRecursive(
	// Map<String, Map<String, List<Integer>>> generatedPairs,
	// List<TableColumn> keyColumns, Table table,
	// Map<String, Double> sortedColumns) {
	// Map<String, Map<String, List<Integer>>> newGeneratedPairs = new
	// HashMap<String, Map<String, List<Integer>>>();
	// // create pairs of keys
	// for (Entry<String, Map<String, List<Integer>>> firstEntry :
	// generatedPairs
	// .entrySet()) {
	// String[] columnIndexestmp = firstEntry.getKey().split("\\|");
	// // if this is the last column then break it, there is no key
	// if (columnIndexestmp.length >= table.getColumns().size() - 1)
	// return;
	// List<String> columnIndexes = new ArrayList<String>();
	// for (String str : columnIndexestmp)
	// columnIndexes.add(str);
	// // count the nm of unique values
	// int uniqeValuesOld = table.getColumns().get(0).getTotalSize();
	// for (Entry<String, List<Integer>> entry : firstEntry.getValue()
	// .entrySet())
	// uniqeValuesOld -= entry.getValue().size();
	// // the second entry
	// for (Entry<String, Double> second : sortedColumns.entrySet()) {
	// int uniqeValues = uniqeValuesOld;
	// int secondColumnIndex = Integer.parseInt(second.getKey());
	// // if the column is alredy included, continue
	// if (columnIndexes.contains(second.getKey()))
	// continue;
	//
	// TableColumn secondColumn = table.getColumns().get(
	// secondColumnIndex);
	// Map<String, List<Integer>> notResolved = new HashMap<String,
	// List<Integer>>();
	// // go through all values
	// for (Entry<String, List<Integer>> nonUniqueEntry : firstEntry
	// .getValue().entrySet()) {
	// List<Integer> newNotUniquesIndexes = new ArrayList<Integer>();
	// List<Integer> nonUniqueIndexes = nonUniqueEntry.getValue();
	// String cellValue = nonUniqueEntry.getKey();
	//
	// // put the unique values from the second column
	// List<String> tmpUniqueValues = new ArrayList<String>();
	// boolean isUnique = true;
	// Map<String, Integer> passedValues = new HashMap<String, Integer>();
	// for (Integer indexFromSecondColumn : nonUniqueIndexes) {
	// String valueTocheck = secondColumn.getValues().get(
	// indexFromSecondColumn);
	// // if already exists then it is not unique
	// if (tmpUniqueValues.contains(valueTocheck)) {
	// isUnique = false;
	//
	// if (passedValues.containsKey(valueTocheck))
	// if (!newNotUniquesIndexes.contains(passedValues
	// .get(valueTocheck)))
	// newNotUniquesIndexes.add(passedValues
	// .get(valueTocheck));
	//
	// } else {
	// passedValues.put(valueTocheck,
	// indexFromSecondColumn);
	// }
	// tmpUniqueValues.add(valueTocheck);
	// }
	//
	// uniqeValues += (nonUniqueIndexes.size() - newNotUniquesIndexes
	// .size());
	// if (!isUnique)
	// notResolved.put(nonUniqueEntry.getKey(),
	// nonUniqueEntry.getValue());
	// }
	//
	// // set the new key if the uniqueness is greater than ..
	// double rank = (double) ((double) uniqeValues / (double) table
	// .getColumns().get(0).getTotalSize());
	// boolean isKey = false;
	// if (rank >= pipeline.getKeyUniqueness())
	// isKey = true;
	//
	// TableColumn firstcolumn = table.getColumns().get(
	// Integer.parseInt(columnIndexes.get(0)));
	// if (firstcolumn.getValuesInfo().containsKey(
	// PipelineConfig.NULL_VALUE.toLowerCase())) {
	// rank = (double) ((double) uniqeValues / (double) ((double) firstcolumn
	// .getTotalSize() - firstcolumn.getValuesInfo().get(
	// PipelineConfig.NULL_VALUE.toLowerCase())));
	// double frac = ((double) (firstcolumn.getValuesInfo().get(
	// PipelineConfig.NULL_VALUE.toLowerCase()) / (double) firstcolumn
	// .getTotalSize()));
	// if (rank >= pipeline.getKeyUniqueness()
	// && frac <= pipeline.getKeyNullValuesFraction())
	// isKey = true;
	// }
	//
	// if (isKey) {
	//
	// for (String str : columnIndexes)
	// keyColumns.add(table.getColumns().get(
	// Integer.parseInt(str)));
	// keyColumns.add(secondColumn);
	// break;
	// }
	// newGeneratedPairs.put(
	// firstEntry.getKey() + "|" + second.getKey(),
	// notResolved);
	// }
	// if (keyColumns.size() > 0)
	// break;
	// }
	// if (keyColumns.size() == 0)
	// getCompundKeysRecursive(newGeneratedPairs, keyColumns, table,
	// sortedColumns);
	// }

	public void setKeyColumns(List<TableColumn> list, Table table) {
		table.setCompaundKeyColumns(list);
		String printStr = "Key columns are: ";
		for (TableColumn column : list) {
			printStr += " | " + column.getHeader();
		}
		printStr = printStr.replaceFirst(" \\| ", "");
		pipeline.getLogger().info(printStr);
	}

	public static Map<String, Double> sortMap(Map<String, Double> inputMap) {

		ValueComparator bvc = new ValueComparator(inputMap);
		TreeMap<String, Double> sortedRankedResults = new TreeMap<String, Double>(
				bvc);
		sortedRankedResults.putAll(inputMap);

		return sortedRankedResults;
	}
}
