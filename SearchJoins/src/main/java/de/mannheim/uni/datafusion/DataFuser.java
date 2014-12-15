package de.mannheim.uni.datafusion;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.TableProcessor.ColumnTypeGuesser;
import de.mannheim.uni.index.IValueIndex;
import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.mongodb.MongoDBReader;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.ExecutionModel;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.statistics.ComplementarityAnalyser;
import de.mannheim.uni.statistics.DistributionOfTablesPerKey;
import de.mannheim.uni.statistics.DistributionOfValuesPerKey;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.units.SubUnit;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;

/**
 * @author petar
 * 
 */
public class DataFuser {

	private IndexManager indexManager;
	private Pipeline pipeline;
	ColumnTypeGuesser typeGuesser;

	private IValueIndex valueIndex;

	public DataFuser(Pipeline pipeline) {
		this.indexManager = new IndexManager(pipeline);

		if (pipeline.getExecutionModel() == ExecutionModel.MSE)
			valueIndex = indexManager;
		else
			valueIndex = new MongoDBReader();

		this.pipeline = pipeline;
		typeGuesser = new ColumnTypeGuesser();
	}

	public DataFuser(Pipeline pipeline, IValueIndex index) {
		this(pipeline);
		valueIndex = index;
	}

	/**
	 * fuses all data in one table
	 * 
	 * @param queryTable
	 * @param joinTabels
	 */
	public Table fuseQueryTableWithEntityTables(Table queryTable,
			List<JoinResult> joinTabels, Map<String, Map<Integer, Double>> tsp) {
		Timer fuse = new Timer("Data fusion");
		long start = System.currentTimeMillis();
		pipeline.getLogger().info("------------Start data fusion----------");
		
		// get completely fused table
		Table fusedTable = null;
		// fusedTable = fuseCompleteTable(queryTable, joinTabels, tsp);
		fusedTable = fuseCompleteTableFast(queryTable, joinTabels, tsp);

		TableDataCleaner dataCleaner = new TableDataCleaner(fusedTable,
				pipeline);

		System.out.println("Evaluating search results");
		DistributionOfTablesPerKey d2 = new DistributionOfTablesPerKey();
		d2.analyse(joinTabels, this.pipeline);

		TableColumn dbpc = null;
		for (TableColumn c : fusedTable.getColumns())
			if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
				dbpc = c;
				break;
			}

		if (dbpc != null) {
			System.out.println("generating distribution of values per key");
			DistributionOfValuesPerKey d = new DistributionOfValuesPerKey();
			d.analyse(fusedTable, this.pipeline, dbpc);

			System.out
					.println("Evaluating complementarity and evidence for DBPedia column "
							+ dbpc.getHeader());

			ComplementarityAnalyser ca = new ComplementarityAnalyser();
			ca.analyse(fusedTable, dbpc, this.pipeline);
			
			fusedTable.writeTableToFile("FullTable_all");
			dataCleaner.removeNullRows(fusedTable, dbpc);
		}


		// print out the table before cleaning it
		fusedTable.writeTableToFile("FullTable");
		fusedTable.writeColumnList("fulltable.columns.csv");
		//fusedTable.writeDetailedData("FullTable.details.csv");
		
		pipeline.getLogger().info(
				"The new table has " + fusedTable.getColumns().size()
						+ " columns");

		if (!pipeline.isSkipCleaning()) {
			pipeline.getLogger().info("Start data cleaning");

			// clean data
			pipeline.getLogger().info(
					"------------------CLEANING THE TABLE------------");
			
			fusedTable = dataCleaner.cleanTable();
			pipeline.getLogger().info(
					"Time for cleaning the table: "
							+ (System.currentTimeMillis() - start));

			pipeline.getLogger().info(
					"The new table after data cleaining has "
							+ fusedTable.getColumns().size() + " columns");
			// write the table to CSV
			fusedTable.writeTableToFile(null);
			fusedTable.writeColumnList("fusedtable.columns.csv");

			pipeline.getLogger().info(
					"Time for data fusion: "
							+ (System.currentTimeMillis() - start));
			fuse.stop();
		}

		return fusedTable;
	}

	private Table generateCompleteTableFromTSP(Table queryTable,
			Map<String, Double> tsp) {

		return null;
	}

	/**
	 * Fuses all join tables without any cleaning
	 * 
	 * @param queryTable
	 * @param joinTabels
	 * @return
	 */
	public Table fuseCompleteTable(Table queryTable,
			List<JoinResult> joinTabels, Map<String, Map<Integer, Double>> tsp) {
		// MongoDBReader mongoReader = null;
		// if (tsp != null)
		// mongoReader = new MongoDBReader();
		Timer t = new Timer("Fuse complete table");
		Table fusedTable = new Table();
		fusedTable.setHeader("AugmentedTable");
		Map<String, TableColumn> newColumns = new HashMap<String, TableColumn>();

		TableColumn keyQueryColumn = queryTable.getCompaundKeyColumns().get(0);

		// add the key column first
		fusedTable.getColumns().add(keyQueryColumn);

		// add the rest of the columns from the query table
		for (TableColumn col : queryTable.getColumns()) {
			if (!col.isKey())
				fusedTable.getColumns().add(col);
		}

		// add the join columns
		int index = 2;
		long lstMsg = System.currentTimeMillis();
		// iterate all key values from the queryTable
		for (int i = 2; i < keyQueryColumn.getValues().size() + 2; i++) {
			String val = keyQueryColumn.getValues().get(i);
			System.out.println(new Date() + ": Fusing " + joinTabels.size()
					+ " joined tables for key value '" + val + "'");
			int joinIndex = 0;
			// find in which joinresults is included the current key value
			for (JoinResult joinR : joinTabels) {
				// find the value in the entries
				for (Entry<IndexEntry, IndexEntry> entry : joinR.getJoinPairs()
						.entrySet()) {
					// if it is the same value; get the rest of the row values
					// and add to the table
					if (entry.getKey().getValue().equals(val)) {

						List<IndexEntry> rowValues = null;
						if (tsp == null)
							/*
							 * rowValues = indexManager.getRowValues(entry
							 * .getValue().getTabelHeader(), entry
							 * .getValue().getEntryID());
							 */
							rowValues = valueIndex.getRowValues(entry
									.getValue().getTabelHeader(), entry
									.getValue().getEntryID());
						else {
							/*
							 * rowValues = mongoReader.getRowValues(entry
							 * .getValue().getTabelHeader(), entry
							 * .getValue().getEntryID());
							 */
							rowValues = valueIndex.getRowValues(entry
									.getValue().getTabelHeader(), entry
									.getValue().getEntryID());

							// filter out columns with low score
							for (int colIndex = rowValues.size() - 1; colIndex >= 0; colIndex--) {
								if (tsp.get(entry.getValue().getTabelHeader())
										.containsKey(colIndex)
										&& tsp.get(
												entry.getValue()
														.getTabelHeader()).get(
												colIndex) == 0)
									rowValues.remove(colIndex);
							}
						}

						// iterate all values and add them to the column
						addValuesToColumn(rowValues, index, newColumns,
								keyQueryColumn.getValues().size() + 2);

					}
				}
				joinIndex++;

				if (System.currentTimeMillis() - lstMsg > 10000) {
					System.out
							.println(new Date() + ":  processing joined table "
									+ joinIndex + "/" + joinTabels.size() + "("
									+ joinR.getHeader() + ")");
					lstMsg = System.currentTimeMillis();
				}
			}
			index++;
		}
		// populate the rest of the column values with nulls
		populateNullValuesInColumns(newColumns, keyQueryColumn.getValues()
				.size() + 2);
		// add all columns
		for (Entry<String, TableColumn> entry : newColumns.entrySet()) {
			fusedTable.getColumns().add(entry.getValue());
		}

		t.stop();
		return fusedTable;

	}

	public Table fuseCompleteTableFast(Table queryTable,
			final List<JoinResult> joinTabels,
			final Map<String, Map<Integer, Double>> tsp) {
		Timer t = new Timer("Fuse complete table");
		Table fusedTable = new Table();
		fusedTable.setHeader("AugmentedTable");
		final Map<String, TableColumn> newColumns = new ConcurrentHashMap<String, TableColumn>();

		final TableColumn keyQueryColumn = queryTable.getCompaundKeyColumns()
				.get(0);

		// add the key column first
		fusedTable.getColumns().add(keyQueryColumn);

		// add the rest of the columns from the query table
		for (TableColumn col : queryTable.getColumns()) {
			if (!col.isKey())
				fusedTable.getColumns().add(col);
		}


		// make sure the list of allowed headers is loaded before starting the
		// parallel loop

		final List<String> headers = pipeline.getHeaderFilter();
                
		if (headers != null && headers.size() > 0)
			pipeline.getLogger().info(
					"Allowing " + headers.size() + " different column headers");
		
		pipeline.getLogger().info(
				"Checking " + joinTabels.size() + " different tables returned from search");
		
		List<JoinResult> resultsToUse1 = joinTabels;
		
        if(headers != null && !headers.isEmpty())
        {
            final Queue<JoinResult> toRemove = new ConcurrentLinkedQueue<JoinResult>();
            final List<JoinResult> toKeep = Collections.synchronizedList(new LinkedList<JoinResult>());
            
            pipeline.getLogger().info("Filtering tables ...");
            // instead of checking every table if it contains some of the headers we
            // can search for the tables that contain some of the headers

            // find all tables that contain some of the headers
            // populate the list of the currently found tables, which will be used
            // for filtering
            if (pipeline.getIndexingMode() == 2) {
                    List<String> tablesToBeUsed = new ArrayList<String>();
                    for (JoinResult jr : joinTabels) {
                            tablesToBeUsed.add(jr.getJoinPairs().values().iterator().next()
                                            .getTabelHeader());
                    }
                    // get the valid tables
                    List<String> validTables = pipeline.getAttributesIndexManager()
                                    .findTablesByHeaders(headers, tablesToBeUsed);

                    pipeline.getLogger().info("Found " + validTables.size() + " tables containing a valid header (complete index)...");
                    
                    for (JoinResult jr : joinTabels) {
                            if (!validTables.contains(jr.getJoinPairs().values().iterator().next().getTabelHeader())) {
                                    toRemove.add(jr);
                            }
                            else
                            	toKeep.add(jr);
                    }
            } else if(pipeline.getIndexingMode() == 1) {
                    try {
                            new Parallel<JoinResult>().foreach(joinTabels,
                                            new Consumer<JoinResult>() {

                                                    public void execute(JoinResult parameter) {
                                                            JoinResult jr = parameter;

                                                            if (jr.getJoinPairs().size() > 0) {
                                                                    String tbl = jr.getJoinPairs().values()
                                                                                    .iterator().next().getTabelHeader();

                                                                    if (!valueIndex.hasHeader(tbl, headers))
                                                                            toRemove.add(jr);
                                                                    else
                                                                    	toKeep.add(jr);
                                                            } else
                                                                    toRemove.add(jr);

                                                    }
                                            });
                    } catch (InterruptedException e1) {
                            // TODO Auto-generated catch block
                            e1.printStackTrace();
                    }

            }
            pipeline.getLogger().info("Removing " + toRemove.size() + " tables");
            
            resultsToUse1 = toKeep;
        }
                
        final List<JoinResult> resultsToUse = resultsToUse1;
        
		pipeline.getLogger().info(
				"Fusing " + resultsToUse.size() + " joined tables");

		// add the join columns
		try {
			new Parallel<Entry<Integer, String>>().foreach(keyQueryColumn
					.getValues().entrySet(),
					new Consumer<Entry<Integer, String>>() {

						public void execute(Entry<Integer, String> parameter) {
							String val = parameter.getValue();
							int index = parameter.getKey();

							int joinIndex = 0;

							// find in which joinresults is included the current
							// key value
							for (JoinResult joinR : resultsToUse) {
								// find the value in the entries
								for (Entry<IndexEntry, IndexEntry> entry : joinR
										.getJoinPairs().entrySet()) {
									// if it is the same value; get the rest of
									// the row values
									// and add to the table
									if (entry.getKey().getValue().equals(val)) {

										List<IndexEntry> rowValues = null;
										if (tsp == null)
											rowValues = valueIndex
													.getRowValues(
															entry.getValue()
																	.getTabelHeader(),
															entry.getValue()
																	.getEntryID(),
															pipeline.getHeaderFilter());
										else {
											rowValues = valueIndex
													.getRowValues(
															entry.getValue()
																	.getTabelHeader(),
															entry.getValue()
																	.getEntryID());

											// filter out columns with low score
											for (int colIndex = rowValues
													.size() - 1; colIndex >= 0; colIndex--) {
												if (tsp.get(
														entry.getValue()
																.getTabelHeader())
														.containsKey(colIndex)
														&& tsp.get(
																entry.getValue()
																		.getTabelHeader())
																.get(colIndex) == 0)
													rowValues.remove(colIndex);
											}
										}

										// iterate all values and add them to
										// the column
										addValuesToColumn(rowValues, index,
												newColumns, keyQueryColumn
														.getValues().size() + 2);

									}
								}
								joinIndex++;
							}
						}
					});
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// populate the rest of the column values with nulls
		//populateNullValuesInColumns(newColumns, keyQueryColumn.getValues().size() + 2);
		// add all columns
		for (Entry<String, TableColumn> entry : newColumns.entrySet()) {
			fusedTable.getColumns().add(entry.getValue());
		}
		
		for (Entry<String, TableColumn> entry : newColumns.entrySet()) {
			TableColumn col = entry.getValue();
			//	set the final datatype
			col.setFinalDataType();
		}

		t.stop();
		return fusedTable;

	}

	/**
	 * populates missing values
	 * 
	 * @param newColumns
	 * @param totlaTableSize
	 */
	private void populateNullValuesInColumns(
			Map<String, TableColumn> newColumns, int totlaTableSize) {
		for (Entry<String, TableColumn> entry : newColumns.entrySet()) {
			TableColumn col = entry.getValue();
			// populate the column with null values
			for (int i = 2; i < totlaTableSize; i++) {
				if (!col.getValues().containsKey(i)
						|| col.getValues().get(i).equals("")) {
					col.addNewValue(i, PipelineConfig.NULL_VALUE,
							true);
				}

			}
			// set the final datatype
			col.setFinalDataType();
		}

	}

	private void addValuesToColumn(List<IndexEntry> rowValues, int index,
			Map<String, TableColumn> newColumns, int totlaTableSize) {
		int tmpColIndex = 0;

		List<String> header_filter = pipeline.getHeaderFilter();

		for (IndexEntry entry : rowValues) {
			// if there is a header filer
			if (header_filter != null)

				// we are looking for exact header matches
				if (pipeline.isSearchExactColumnHeaders()) {
					if (!header_filter.contains(entry.getColumnHeader()))
						continue;
				}
				// we are looking for normalised header matches
				else if (!header_filter.contains(StringNormalizer.clearString(
						entry.getColumnHeader(), true,
						pipeline.isSearchStemmedColumnHeaders())))
					continue;

			String columnTableHeader = entry.getColumnHeader() + "|"
					+ entry.getFullTablePath();
			if (entry.getColumnIndex() != -1)
				columnTableHeader += "|" + entry.getColumnIndex();
			else
				columnTableHeader += "|" + tmpColIndex;

			if (entry.isPrimaryKey())
				continue;
			tmpColIndex++;
			SubUnit unit = new SubUnit();
			String cleanedValue = ConvertFileToTable.simpleStringNormalization(
					entry.getValue(), false);

			if (cleanedValue == null)
				cleanedValue = "";

			ColumnDataType valueType = typeGuesser.guessTypeForValue(
					cleanedValue, entry.getColumnHeader(), true, unit);

			// if it is a string, clean it completely
			if (valueType == ColumnDataType.string)
				cleanedValue = StringNormalizer.clearString(entry.getValue(),
						true);
			if (cleanedValue.equals(""))
				cleanedValue = PipelineConfig.NULL_VALUE;
			TableColumn column = new TableColumn();
			if (newColumns.containsKey(columnTableHeader)) {
				column = newColumns.get(columnTableHeader);
				column.addNewValue(index, cleanedValue, true);
				column.addNewDataType(valueType);
				if (unit != null && valueType == ColumnDataType.unit)
					column.addNewUnit(index, unit);
			} else {

				column.setHeader(entry.getColumnHeader());
				column.setDataSource(entry.getFullTablePath());
				column.addNewValue(index, cleanedValue, true);
				column.addNewDataType(valueType);
				if (unit != null && valueType == ColumnDataType.unit)
					column.addNewUnit(index, unit);
				newColumns.put(columnTableHeader, column);

			}
		}

	}
}
