package de.mannheim.uni.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import de.mannheim.uni.units.SubUnit;
import de.mannheim.uni.units.Unit;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class TableColumn
	implements java.io.Serializable
{
	public static enum ColumnDataType {
		numeric, string, coordinate, date, link, bool, unknown, unit, list
	};

	private int nmNtUniqueValues;

	public void setNmNtUniqueValues(int nmNtUniqueValues) {
		this.nmNtUniqueValues = nmNtUniqueValues;
	}

	public int getNmNtUniqueValues() {
		return nmNtUniqueValues;
	}

	private int totalSize;

	private String header;

	private ColumnDataType dataType;

	Map<Integer, String> values;

	// private Map<String, Integer> indexesByValue;

	// keeps units for each column value
	private Map<Integer, SubUnit> units;

	// keeps base units detection
	private Map<Unit, Integer> baseUnitsCount;

	private Unit baseUnit;

	private double averageValuesSize;

	private int totalValueSize;

	private String cleaningInfo;

	private String dataSource;

	public String getCleaningInfo() {
		return cleaningInfo;
	}

	public void setCleaningInfo(String cleaningInfo) {
		this.cleaningInfo = cleaningInfo;
	}

	public Map<Unit, Integer> getBaseUnitsCount() {
		return baseUnitsCount;
	}

	public void setBaseUnitsCount(Map<Unit, Integer> baseUnitsCount) {
		this.baseUnitsCount = baseUnitsCount;
	}

	public String getDataSource() {
		return dataSource;
	}

	public void setDataSource(String dataSource) {
		this.dataSource = dataSource;
	}

	public void setAverageValuesSize(double averageValuesSize) {
		this.averageValuesSize = averageValuesSize;
	}

	public double getAverageValuesSize() {
		return averageValuesSize;
	}

	public Map<Integer, SubUnit> getUnits() {
		return units;
	}

	public void setUnits(Map<Integer, SubUnit> units) {
		this.units = units;
	}

	// public Map<String, Integer> getIndexesByValue() {
	// return indexesByValue;
	// }
	//
	// public void setIndexesByValue(Map<String, Integer> indexesByValue) {
	// this.indexesByValue = indexesByValue;
	// }

	public Map<ColumnDataType, Integer> getColumnValuesTypes() {
		return columnValuesTypes;
	}

	public void setColumnValuesTypes(
			Map<ColumnDataType, Integer> columnValuesTypes) {
		this.columnValuesTypes = columnValuesTypes;
	}

	public boolean isKey() {
		return isKey;
	}

	public void setKey(boolean isKey) {
		this.isKey = isKey;
	}

	public Unit getBaseUnit() {
		return baseUnit;
	}

	public void setBaseUnit(Unit baseUnit) {
		this.baseUnit = baseUnit;
	}

	private Map<ColumnDataType, Integer> columnValuesTypes;

	private Map<String, Integer> valuesInfo;

	private boolean isKey;

	// private Map<String, List<Integer>> nonUniqueValues;
	//
	// public Map<String, List<Integer>> getNonUniqueValues() {
	// return nonUniqueValues;
	// }

	// public void setNonUniqueValues(Map<String, List<Integer>>
	// nonUniqueValues) {
	// this.nonUniqueValues = nonUniqueValues;
	// }

	public String getHeader() {
		return header;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public ColumnDataType getDataType() {
		return dataType;
	}

	public void setDataType(ColumnDataType dataType) {
		this.dataType = dataType;
	}

	public Map<Integer, String> getValues() {
		return values;
	}

	public void setValues(Map<Integer, String> values) {
		this.values = values;
	}

	public Map<String, Integer> getValuesInfo() {
		return valuesInfo;
	}

	public void setValuesInfo(Map<String, Integer> valuesInfo) {
		this.valuesInfo = valuesInfo;
	}

	public int getTotalSize() {
		return totalSize;
	}

	public void setTotalSize(int totalSize) {
		this.totalSize = totalSize;
	}

	public TableColumn() {
		values = new HashMap<Integer, String>();
		header = "";
		dataType = ColumnDataType.string;
		valuesInfo = new HashMap<String, Integer>();
		columnValuesTypes = new HashMap<ColumnDataType, Integer>();
		// nonUniqueValues = new HashMap<String, List<Integer>>();
		// indexesByValue = new HashMap<String, Integer>();
		nmNtUniqueValues = 0;
		units = new HashMap<Integer, SubUnit>();
		totalValueSize = 0;
		baseUnitsCount = new HashMap<Unit, Integer>();
	}

	public void addNewUnit(int index, SubUnit subUnit) {
		units.put(index, subUnit);

		synchronized(baseUnitsCount)
		{
			int value = 1;
			if (baseUnitsCount.containsKey(subUnit.getBaseUnit()))
				value += baseUnitsCount.get(subUnit.getBaseUnit());
			baseUnitsCount.put(subUnit.getBaseUnit(), value);
		}
	}

	public double getColumnUniqnessRank() {
		// double rank = (double) ((double) valuesInfo.size() / (double)
		// totalSize);
		int uniqueValues = computeUniqueValues();
		double rank = (double) ((double) (uniqueValues) / (double) totalSize);
		if (valuesInfo.containsKey(PipelineConfig.NULL_VALUE))
			rank = rank
					- ((double) ((double) valuesInfo
							.get(PipelineConfig.NULL_VALUE) / (double) totalSize));
		return rank;
	}

	public double getColumnUniqnessRankWoNull() {
		// double rank = (double) ((double) valuesInfo.size() / (double)
		// totalSize);
		int uniqueValues = computeUniqueValues();
		double rank = (double) ((double) (uniqueValues) / (double) totalSize);
		if (valuesInfo.containsKey(PipelineConfig.NULL_VALUE))
			rank = (double) ((double) (uniqueValues) / (double) ((double) totalSize - valuesInfo
					.get(PipelineConfig.NULL_VALUE)));
		return rank;
	}

	public int computeUniqueValues() {
		int uniqueValues = totalSize;
		for (Entry<String, Integer> entry : valuesInfo.entrySet()) {
			if (entry.getValue() > 1)
				uniqueValues -= entry.getValue();
		}
		return uniqueValues;
	}

	public void addNewValue(int index, String value, boolean allowNulls) {
		if (value.equalsIgnoreCase(PipelineConfig.NULL_VALUE)
				|| value.equals(""))
			value = PipelineConfig.NULL_VALUE;
		// indexesByValue.put(value, index);
		// set the multiplicity of the value
		int valueMultiplicity = 1;
		synchronized(valuesInfo)
		{
			if (valuesInfo.containsKey(value))
				valueMultiplicity += valuesInfo.get(value);
			valuesInfo.put(value, valueMultiplicity);
		}
		if (allowNulls) {
			values.put(index, value);
		} else if (!value.equalsIgnoreCase(PipelineConfig.NULL_VALUE)
				&& !value.equals("")) {
			values.put(index, value);
		}

		totalSize++;
		// set average value size
		totalValueSize += value.length();
		averageValuesSize = totalValueSize / totalSize;

		// if (nonUniqueValues.containsKey(value)) {
		// List<Integer> nonUniqueIndexes = nonUniqueValues.get(value);
		// nonUniqueIndexes.add(index);
		// nonUniqueValues.put(value, nonUniqueIndexes);
		// nmNtUniqueValues++;
		// } else {
		// List<Integer> nonUniqueIndexes = new ArrayList<Integer>();
		// nonUniqueIndexes.add(index);
		// nonUniqueValues.put(value, nonUniqueIndexes);
		// }
	}

	public void setFinalDataType() {
		setDataType(ColumnDataType.string);
		if (columnValuesTypes.size() > 0) {
			int maxCount = columnValuesTypes.values().iterator().next();
			ColumnDataType type = columnValuesTypes.keySet().iterator().next();
			for (Entry<ColumnDataType, Integer> entry : columnValuesTypes
					.entrySet()) {
				if (entry.getValue() > maxCount) {
					maxCount = entry.getValue();
					type = entry.getKey();
				}
			}
			// check if it is boolean if it contains different values than
			// binary
			if (type == ColumnDataType.bool) {
				for (String val : values.values())
					if (!val.equals("0") && !val.equals("1")
							&& !val.toLowerCase().equals("true")
							&& !val.toLowerCase().equals("false")) {
						type = ColumnDataType.numeric;
					}
			}
			setDataType(type);
			if (type == ColumnDataType.unit) {
				setFinalUnit();
			}
		}

		//System.out.println("column " + header + " has datatype: " + getDataType().toString());
	}

	private void setFinalUnit() {
		int maxUnit = 0;
		Unit baseUnit = null;

		for (Entry<Unit, Integer> entry : baseUnitsCount.entrySet()) {
			if (maxUnit < entry.getValue()) {
				maxUnit = entry.getValue();
				baseUnit = entry.getKey();
			}
		}
		this.baseUnit = baseUnit;

	}

	public void addNewDataType(ColumnDataType valueType) {

		synchronized(columnValuesTypes)
		{
			int newValue = 1;
			if (columnValuesTypes.containsKey(valueType)) {
				newValue += columnValuesTypes.get(valueType);
			}
			columnValuesTypes.put(valueType, newValue);
		}
	}

	@Override
	public int hashCode() {
		return new HashCodeBuilder(17, 31).append(header).append(dataSource)
				.toHashCode();

	}

	@Override
	public boolean equals(Object obj) {
		if (obj == null)
			return false;
		if (obj == this)
			return true;
		if (!(obj instanceof TableColumn))
			return false;

		TableColumn rhs = (TableColumn) obj;
		return new EqualsBuilder().append(header, rhs.header)
				.append(dataSource, rhs.dataSource).isEquals();
	}

}
