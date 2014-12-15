package de.mannheim.uni.datafusion;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.Matcher;
import de.mannheim.uni.schemamatching.instance.InstanceBasedMatcher;
import de.mannheim.uni.schemamatching.label.TablesLabeledBasedMatcher;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.units.SubUnit;
import de.mannheim.uni.units.Unit;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;

/**
 * @author petar
 * 
 */
public class TableDataCleaner {
	private Table table;
	private InstanceBasedMatcher instanceMatcher;
	private TablesLabeledBasedMatcher labelMatcher;
	private Pipeline pipeline;

	public TableDataCleaner(Table table, Pipeline pipeline) {
		this.pipeline = pipeline;
		this.table = table;
		instanceMatcher = new InstanceBasedMatcher();
		labelMatcher = new TablesLabeledBasedMatcher(pipeline, table);
		// TODO Auto-generated constructor stub
	}

	public Table cleanTable() {
		Timer t = new Timer("Clean fused table");

		// normalize units
		normalizeUnits(table);

		// find duplicates with schema matching
		try {
			if (pipeline.isUseExperimentalFusing()) {
				// test: try a different matching approach
				Matcher m = new Matcher(pipeline);
				DuplicateResolver dResolver = new DuplicateResolver(pipeline);
				//ExperimentalDuplicateResolver dResolver = new ExperimentalDuplicateResolver(
						//pipeline);
				TableObjectsMatch mEmpty = new TableObjectsMatch();

				// use combined label & instance based matching first
				TableObjectsMatch matching = m.matchColumns(table);

				table = dResolver.resolveDuplicates(table, matching, mEmpty);

				// run instance-based matching again to check columns that were
				// merged by label-based similarity
				// HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>>
				// scores = m
				// .matchInstances(table);
				// m.printMatchingResult(scores,
				// pipeline.getComplementaryScore(),
				// "instance2_scores.csv");
				//
				// matching = m.decideComplementaryObjectMatching(scores,
				// table);
				// m.printTableObjectsMatch(matching, "instance2_decided.csv");
				//
				// table = dResolver.resolveDuplicates(table, matching, mEmpty);

				// TableObjectsMatch matching = m.clusterColumns(table);
				// m.printTableObjectsMatch(matching, "clusters.csv");
				// table = dResolver.resolveDuplicates(table, matching, mEmpty);

			} else {
				pipeline.getLogger().info(
						"Detecting duplicates with instance matching");
				long start = System.currentTimeMillis();
				TableObjectsMatch fusedDuplicates = instanceMatcher
						.checkForDuplicates(table, pipeline);
				pipeline.getLogger().info(
						"Instance based matching took "
								+ (System.currentTimeMillis() - start) / 1000
								+ "s");
				if (fusedDuplicates.getColumns().size() > 0) {
					pipeline.getLogger().info(
							"Detecting duplicates with label based matching");
					start = System.currentTimeMillis();
					TableObjectsMatch fusedDuplicates2 = labelMatcher
							.checkForDuplicates(table, fusedDuplicates, pipeline);
					pipeline.getLogger().info(
							"Label based matching took "
									+ (System.currentTimeMillis() - start)
									/ 1000 + "s");
					pipeline.getLogger().info("Merging duplicates");
					DuplicateResolver dResolver = new DuplicateResolver(
							pipeline);
					start = System.currentTimeMillis();
					table = dResolver.resolveDuplicates(table, fusedDuplicates,
							fusedDuplicates2);
					pipeline.getLogger().info(
							"Merging took "
									+ (System.currentTimeMillis() - start)
									/ 1000 + "s");
				}
			}

			// write the table to CSV
			table.writeTableToFile("fusedtable_with_nulls");
			table.writeColumnList("fusedtable_with_nulls.columns.csv");

			// filter columns by column density
			pipeline.getLogger().info("Removeing null columns");
			table = filterColumnsByColumnDensity(table);
			
			//table.writeTableToFile("fusedtable_with_null_rows");
			
			// filter rows by row density
			pipeline.getLogger().info("Removing null rows");
			table = filterRowsByRowDensity(table);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		t.stop();
		return table;
	}

	private void normalizeUnits(Table table) {
		for (TableColumn column : table.getColumns()) {
			if (column.getDataType() == ColumnDataType.unit
					&& column.getBaseUnit() != null
					&& !column.getBaseUnit().getName().equals("Currency")) {
				normalizeColumnUnit(column);

			} else if (column.getDataType() == ColumnDataType.numeric) {
				normalizeColumnNumeric(column);
			}
		}

	}

	/**
	 * Cleans the numeric values of spaces and characters
	 * 
	 * @param column
	 */
	private void normalizeColumnNumeric(TableColumn column) {
		for (Entry<Integer, String> entry : column.getValues().entrySet()) {
			if (entry.getValue()
					.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
				continue;
			String newValue = entry.getValue().replaceAll("[^\\d.]", "");

			// remove multiple dots
			if (newValue.matches(".*\\..*\\..*"))
				newValue = newValue.replaceAll("\\.", "");
			column.getValues().put(entry.getKey(), newValue);
		}

	}

	/**
	 * normalizes units to
	 * 
	 * @param column
	 */
	private void normalizeColumnUnit(TableColumn column) {
		Unit baseUnit = column.getBaseUnit();
		for (Entry<Integer, String> entry : column.getValues().entrySet()) {
			SubUnit subUnit = column.getUnits().get(entry.getKey());
			if (subUnit != null && subUnit.getNewValue() != null)
				column.getValues().put(entry.getKey(), subUnit.getNewValue());
		}
		try {
			if (baseUnit != null) {
				column.setCleaningInfo("Unit was normalized to "
						+ baseUnit.getName() + " using unit "
						+ baseUnit.getMainUnit().getAbbrevations().get(0));
				pipeline.getLogger().info(
						"Unit was normalized to "
								+ baseUnit.getName()
								+ " using unit "
								+ baseUnit.getMainUnit().getAbbrevations()
										.get(0));
			}
		} catch (Exception e) {
			e.printStackTrace();// the base unt might be null
		}

	}

	/**
	 * removes columns with column density higher than the config density
	 * 
	 * @param fusedTable
	 */
	public Table filterColumnsByColumnDensity(Table fusedTable) {
		if(pipeline.getDataFusionColumnDensity()==1)
			return fusedTable;
		
		Timer t = new Timer("Filter columns by density");
		List<TableColumn> colsToremove = new ArrayList<TableColumn>();
		for (TableColumn column : fusedTable.getColumns()) {
			double columnDensity = 0.0;
			if (column.getValuesInfo().containsKey(
					PipelineConfig.NULL_VALUE)) {
				columnDensity = ((double) (column.getValuesInfo().get(
						PipelineConfig.NULL_VALUE) / (double) column
						.getTotalSize()));
			} else {
				int totalNulls = 0;
				for (String value : column.getValues().values()) {
					if (value.toLowerCase().equals(
							PipelineConfig.NULL_VALUE))
						totalNulls++;
				}
				columnDensity = (double) ((double) totalNulls / (double) column
						.getValues().size());
			}
			if (columnDensity > pipeline.getDataFusionColumnDensity()) {
				colsToremove.add(column);
				pipeline.getLogger().info(
						"Column removed because of NULL values: "
								+ column.getHeader() + "(density is "
								+ columnDensity + ")");
			}
		}
		for (TableColumn c : colsToremove) {
			fusedTable.getColumns().remove(c);
		}
		t.stop();
		return fusedTable;
	}

	/**
	 * removes rows with row density higher than the config density
	 * 
	 * @param fusedTable
	 */
	public Table filterRowsByRowDensity(final Table fusedTable) {
		if(pipeline.getDataFusionRowDensity()==1)
			return fusedTable;
		
		Timer t = new Timer("Filter rows by density");
		
		//final List<Integer> rowsToremove = Collections.synchronizedList(new LinkedList<Integer>());
		final ConcurrentLinkedQueue<Integer> rowsToRemove = new ConcurrentLinkedQueue<Integer>();
		
		int numRows = 0;
		for(TableColumn c : fusedTable.getColumns())
			if(c.isKey())
			{
				numRows = c.getValues().size();
				break;
			}
		//int numRows = fusedTable.getCompaundKeyColumns().get(0).getValues().size();
		
		try {
			Parallel.forLoop(pipeline.getInstanceStartIndex(), numRows + pipeline.getInstanceStartIndex(), new Consumer<Integer>() {

				public void execute(Integer parameter) {
					int row = parameter;
					double density = 0.0;
					
					for(TableColumn c : fusedTable.getColumns())
						if(!c.isKey() && (!c.getValues().containsKey(row) || c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE)))
							density++;
					
					density = density / ((double)fusedTable.getColumns().size()-1);
					
					if (density > pipeline.getDataFusionRowDensity()) {
						rowsToRemove.add(row);
						pipeline.getLogger().info(
								"Row removed because of NULL values: "
										+ row + "(density is "
										+ density + ")");
					}
					
				}
			});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*for(int row = pipeline.getInstanceStartIndex(); row<numRows + pipeline.getInstanceStartIndex(); row++) {
			double density = 0.0;
			
			for(TableColumn c : fusedTable.getColumns())
				if(!c.isKey() && (!c.getValues().containsKey(row) || c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE)))
					density++;
			
			density = density / ((double)fusedTable.getColumns().size()-1);
			
			if (density > pipeline.getDataFusionRowDensity()) {
				rowsToremove.add(row);
				pipeline.getLogger().info(
						"Row removed because of NULL values: "
								+ row + "(density is "
								+ density + ")");
			}
		}*/
		int removed = rowsToRemove.size();
		for (Integer row : rowsToRemove) {
			for(TableColumn c : fusedTable.getColumns())
				c.getValues().remove(row);
		}
		
		pipeline.getLogger().info("Removed " + removed + "/" + numRows + " low density rows");
		
		t.stop();
		return fusedTable;
	}
	
	public Table removeNullRows(final Table fusedTable, final TableColumn referenceColumn) {
		Timer t = new Timer("Remove null rows");
		
		pipeline.getLogger().log(Level.INFO, "Removing null rows ...");
		
		//final List<Integer> rowsToremove = Collections.synchronizedList(new LinkedList<Integer>());
		final ConcurrentLinkedQueue<Integer> rowsToRemove  = new ConcurrentLinkedQueue<Integer>();
		
		int numRows = 0;
		for(TableColumn c : fusedTable.getColumns())
			if(c.isKey())
			{
				numRows = c.getValues().size();
				break;
			}
		//int numRows = fusedTable.getCompaundKeyColumns().get(0).getValues().size();

		try {
			new Parallel<Integer>().foreach(fusedTable.getFirstKey().getValues().keySet(), new Consumer<Integer>() {
			//Parallel.forLoop(pipeline.getInstanceStartIndex(), numRows + pipeline.getInstanceStartIndex(), new Consumer<Integer>() {

				public void execute(Integer parameter) {
					int row = parameter;
					boolean hasValue=false;
					for(TableColumn c : fusedTable.getColumns())
						if(!c.isKey() 
								&& c!=referenceColumn 
								&& c.getValues().containsKey(row) 
								&& !c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE) 
								&& !c.getValues().get(row).equals(""))
						{
							hasValue=true;
							break;
						}
					
					if(!hasValue)
					{
						rowsToRemove.add(row);
					}
				}
			});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		/*for(int row = pipeline.getInstanceStartIndex(); row<numRows + pipeline.getInstanceStartIndex(); row++) {
			boolean hasValue=false;
			for(TableColumn c : fusedTable.getColumns())
				if(!c.isKey() && c!=referenceColumn && c.getValues().containsKey(row) && !c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE))
				{
					hasValue=true;
					break;
				}
			
			if(!hasValue)
			{
				rowsToremove.add(row);
			}
		}*/
		int toRemove = rowsToRemove.size();
		for (Integer row : rowsToRemove) {
			for(TableColumn c : fusedTable.getColumns())
				c.getValues().remove(row);
		}
		
		pipeline.getLogger().log(Level.INFO, "Removed "+ toRemove + "/" + numRows + " null rows ...");
		
		t.stop();
		return fusedTable;
	}
	
	public static void main(String[] args) {
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);
		System.out.println(sim.score("River", "rdf-schema#label"));
	}
}
