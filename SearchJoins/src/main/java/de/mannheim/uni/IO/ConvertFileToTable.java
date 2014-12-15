package de.mannheim.uni.IO;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.zip.GZIPInputStream;

import org.apache.commons.io.IOUtils;

import com.hp.hpl.jena.reasoner.rulesys.impl.ConsumerChoicePointFrame;

import au.com.bytecode.opencsv.CSVReader;
import de.mannheim.uni.TableProcessor.ColumnTypeGuesser;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.TableStats;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.units.SubUnit;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Producer;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class ConvertFileToTable {

	public static enum ReadTableType {
		lod, web, search, test, webarchive
	};

	public static final String delimiter = "\",\"";
	private Pipeline pipeline;
	private ColumnTypeGuesser typeGuesser;
	private Boolean cleanHeader = true;

	public void setCleanHeader(Boolean clean) {
		cleanHeader = clean;
	}

	private char overrideDelimiter = 0;

	public void setOverrideDelimiter(char delimiter) {
		overrideDelimiter = delimiter;
	}

	public ConvertFileToTable(Pipeline pipeline) {
		this.pipeline = pipeline;
		// create typeguesser
		typeGuesser = new ColumnTypeGuesser();
	}

	/**
	 * Use to read LOD table for indexing uses simple fileStream to read the
	 * file Use this one if the file has more than 200000 lines
	 * 
	 * @param path
	 * @return
	 */
	public Table readLODtableForIndexing(String path) {

		// create new table
		Table table = new Table();
		// TODO: take care of the header of the table
		table.setHeader(path);

		try {
			GZIPInputStream gzip = new GZIPInputStream(
					new FileInputStream(path));
			BufferedReader in = new BufferedReader(new InputStreamReader(gzip));
			// File fileDir = new File(path);
			//
			// BufferedReader in = new BufferedReader(new InputStreamReader(
			// new FileInputStream(fileDir), "UTF8"));
			// read headers
			String fileLine = in.readLine();
			String[] columnNames;
			String[] columntypes;
			// read the column names
			columnNames = fileLine.split(delimiter);
			// check if it is valid size
			if (columnNames.length < pipeline.getMinCols())
				return null;
			// if (columnNames.length > 0)
			// columnNames[0] = "URI";
			// read the datatypes
			fileLine = in.readLine();
			fileLine = in.readLine();
			columntypes = fileLine.split(delimiter);
			int i = 0;
			for (String columnName : columnNames) {
				String datatype = columntypes[i];
				TableColumn column = new TableColumn();
				column.setHeader(columnName.replace("\"", ""));
				datatype = datatype.replace("\"", "");
				// if we don't need non strings atts we don't need to read them
				// if (pipeline.isRemoveNonStrings() == true) {
				// if (datatype.equals(PipelineConfig.SCHEMA_STRING))
				//
				// {
				// column.setDataType(ColumnDataType.string);
				//
				// }
				// } else {
				column.setDataType(ColumnDataType.unknown);
				// }
				table.getColumns().add(column);
				i++;
			}
			// skip the last header
			fileLine = in.readLine();
			String[] nextLine;
			long start = System.currentTimeMillis();

			// the absolute row number in the file
			int rowIndex = 5;
			while ((fileLine = in.readLine()) != null) {
				fileLine = fileLine.substring(1, fileLine.length() - 1);
				nextLine = fileLine.split(delimiter);

				i = 0;
				int columnIndex = 0;
				if (rowIndex % 1000 == 0) {
					// System.out.println(rowIndex);

					System.out.println(rowIndex + " rows were read for:"
							+ (System.currentTimeMillis() - start));
				}
				for (String columnValue : nextLine) {
					String columnHeader = columnNames[i];
					String datatype = columntypes[i];
					// choose only first value if the label is a list
					// if (columnHeader.equals("rdf-schema#label")) {
					// if (columnValue.startsWith("{")
					// && columnValue.endsWith("}")
					// && columnValue.contains("|")) {
					// columnValue = columnValue.substring(1,
					// columnValue.indexOf("|"));
					// }
					// }

					String columnValueNormalized = simpleStringNormalization(
							columnValue, true);
					if (columnValueNormalized
							.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
						columnValue = columnValueNormalized;
					// add the nonnormalized value
					table.getColumns().get(columnIndex)
							.addNewValue(rowIndex, columnValue, false);

					// guess the type
					if (!columnValueNormalized
							.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {
						ColumnDataType valueType = typeGuesser
								.guessTypeForValue(columnValueNormalized, table
										.getColumns().get(columnIndex)
										.getHeader(), false, null);
						if (checkIfList(columnValue)) {
							valueType = ColumnDataType.list;
						}
						table.getColumns().get(columnIndex)
								.addNewDataType(valueType);
					}

					columnIndex++;
					i++;
				}
				rowIndex++;
			}

			// }
			in.close();
			gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			table = null;
			e.printStackTrace();
		}

		return table;
	}

	/**
	 * Use to read LOD table for indexing value by value uses simple fileStream
	 * to read the file Use this one if the file has more than 200000 lines
	 * 
	 * @param path
	 * @return
	 */
	public TableStats readLODtableForIndexingByValue(String path) {

		long totalTimeIndexing = 0;
		TableStats stat = new TableStats();
		// create new table
		Table table = new Table();
		// TODO: take care of the header of the table
		table.setHeader(path);

		try {
			GZIPInputStream gzip = new GZIPInputStream(
					new FileInputStream(path));
			BufferedReader in = new BufferedReader(new InputStreamReader(gzip));
			// File fileDir = new File(path);
			//
			// BufferedReader in = new BufferedReader(new InputStreamReader(
			// new FileInputStream(fileDir), "UTF8"));
			// read headers
			String fileLine = in.readLine();
			String[] columnNames;
			String[] columntypes;
			// read the column names
			columnNames = fileLine.split(delimiter);
			// if (columnNames.length > 0)
			// columnNames[0] = "URI";
			// read the datatypes
			fileLine = in.readLine();
			fileLine = in.readLine();
			columntypes = fileLine.split(delimiter);
			int i = 0;
			for (String columnName : columnNames) {
				String datatype = columntypes[i];
				TableColumn column = new TableColumn();
				column.setHeader(columnName.replace("\"", ""));
				datatype = datatype.replace("\"", "");

				column.setDataType(ColumnDataType.unknown);

				table.getColumns().add(column);
				i++;
			}
			// skip the last header
			fileLine = in.readLine();
			String[] nextLine;
			long start = System.currentTimeMillis();
			// the absolute row number in the file
			int rowIndex = 5;
			while ((fileLine = in.readLine()) != null) {
				fileLine = fileLine.substring(1, fileLine.length() - 1);
				nextLine = fileLine.split(delimiter);

				i = 0;
				int columnIndex = 0;
				if (rowIndex % 1000 == 0) {
					// System.out.println(rowIndex);
					totalTimeIndexing = System.currentTimeMillis() - start;
					System.out.println(rowIndex + " rows were indexed for:"
							+ totalTimeIndexing);
				}
				boolean isKey = false;
				for (String columnValue : nextLine) {
					String columnHeader = columnNames[i];
					String datatype = columntypes[i];
					// choose only first value if the label is a list
					if (columnHeader.equals("rdf-schema#label")) {
						isKey = true;
						if (columnValue.startsWith("{")
								&& columnValue.endsWith("}")
								&& columnValue.contains("|")) {
							columnValue = columnValue.substring(1,
									columnValue.indexOf("|"));
						}
					}
					String columnValueNormalized = StringNormalizer
							.clearString(columnValue, true);

					if (columnValueNormalized
							.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {
						stat.setNmNulls(stat.getNmNulls() + 1);
						columnIndex++;
						i++;
						continue;
					}

					ColumnDataType valueType = typeGuesser.guessTypeForValue(
							simpleStringNormalization(columnValue, true), table
									.getColumns().get(columnIndex).getHeader(),
							false, null);

					if (checkIfList(columnValue)) {
						valueType = ColumnDataType.list;
					}
					if (valueType.toString() == "string"
							|| valueType.toString() == "list") {

					} else {
						columnValueNormalized = ConvertFileToTable
								.simpleStringNormalization(columnValue, true);
					}
					IndexEntry entry = new IndexEntry();

					entry.setFullTablePath(path);
					entry.setColumnDataType(ColumnDataType.unknown.toString());
					entry.setColumnDistinctValues(0);
					entry.setColumnHeader(columnHeader);
					entry.setEntryID(rowIndex);
					entry.setOriginalValue(columnValue);
					entry.setValue(columnValueNormalized);
					entry.setPrimaryKey(isKey);
					entry.setTabelHeader(table.getHeader());
					entry.setValueMultiplicity(0);
					entry.setTableCardinality(0);

					pipeline.getIndexManager().indexValue(entry);

					columnIndex++;
					i++;
				}

				stat.setNmCols(columnIndex);
				rowIndex++;
			}
			System.out.println("The whole table was indexed for:"
					+ totalTimeIndexing);
			stat.setHeader(path);
			stat.setNmRows(rowIndex - 5);
			in.close();
			gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			table = null;
			e.printStackTrace();
		}

		return stat;
	}

	private Table readWebTableForTesting(String tablePath) {
		Table table = new Table();
		// TODO: take care of the header of the table
		table.setHeader(tablePath);
		CSVReader reader = null;

		try {
			// create reader
			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(
					tablePath));
			reader = new CSVReader(new InputStreamReader(gzip, "UTF-8"));

			// read headers
			String[] columnNames;
			// read the column names
			columnNames = reader.readNext();

			String[] columnTypes;
			// read the column names
			columnTypes = reader.readNext();

			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				column.setHeader(simpleStringNormalization(columnName, false));
				// TODO take care of the type
				try {
					column.setDataType(ColumnDataType.valueOf(columnTypes[i]));
				} catch (Exception e) {
					column.setDataType(ColumnDataType.unit);
					// column.setBaseUnit();
				}
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			int rowNumber = 3;

			String[] values = null;

			while ((values = reader.readNext()) != null) {
				if (rowNumber % 100 == 0)
					System.out.println(rowNumber);
				i = 0;
				int columnIndex = 0;

				for (String columnValue : values) {

					// use simple normalized
					table.getColumns().get(columnIndex)
							.addNewValue(rowNumber, columnValue, true);

					columnIndex++;
					i++;
				}
				rowNumber++;
			}
			gzip.close();
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			table = null;
		}

		return table;
	}

	/**
	 * Read web table
	 * 
	 * @param tablePath
	 * @param startInstanceRow
	 *            absolute index of the first instance
	 * @return
	 */
	public Table readTableForSearch(String tablePath, final int startInstanceRow) {

		// create new table
		final Table table = new Table();
		Table result = table;
		// TODO: take care of the header of the table
		table.setHeader(tablePath);
		// CSVReader reader = null;

		try {
			// create reader
			CSVReader reader1 = null;
			GZIPInputStream gzip = null;

			// read both csv and gz
			if (tablePath.endsWith(".gz")) {
				gzip = new GZIPInputStream(new FileInputStream(tablePath));
				reader1 = new CSVReader(new InputStreamReader(gzip, "UTF-8"));
			} else {
				reader1 = new CSVReader(new InputStreamReader(
						new FileInputStream(tablePath), "UTF-8"));
			}
			final CSVReader reader = reader1;

			// read headers
			String[] columnNames;
			// read the column names
			columnNames = reader.readNext();

			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				column.setHeader(simpleStringNormalization(columnName, false));
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			// int rowNumber = startInstanceRow;

			// String[] values = null;

			for (i = 1; i < startInstanceRow - 1; i++)
				reader.readNext();

			new Parallel<Pair<Integer, String[]>>()
					.producerConsumer(
							new de.mannheim.uni.utils.concurrent.Producer<Pair<Integer, String[]>>() {

								@Override
								public void execute() {
									int row = startInstanceRow;

									String[] values = null;

									try {
										while ((values = reader.readNext()) != null) {
											produce(new Pair<Integer, String[]>(
													row++, values));
										}
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}, new Consumer<Pair<Integer, String[]>>() {

								public void execute(
										Pair<Integer, String[]> parameter) {
									int rowNumber = parameter.getFirst();
									String[] values = parameter.getSecond();

									if (rowNumber % 100 == 0)
										System.out.println(rowNumber);
									int i = 0;
									int columnIndex = 0;
									ColumnDataType valueType = ColumnDataType.string;
									for (String columnValue : values) {
										// clean the value completely
										String columnValueNormalized = columnValue;

										if (columnValueNormalized.equals(""))
											columnValueNormalized = PipelineConfig.NULL_VALUE;
										else {
											columnValueNormalized = StringNormalizer
													.clearString(columnValue,
															true);
											if (columnValueNormalized
													.equals(""))
												columnValueNormalized = PipelineConfig.NULL_VALUE;
										}

										if (!columnValueNormalized
												.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {
											// maybe it is a unit
											SubUnit unit = new SubUnit();
											String simpleNormalized = simpleStringNormalization(
													columnValue, true);

											// guess the type
											valueType = typeGuesser
													.guessTypeForValue(
															simpleNormalized,
															table.getColumns()
																	.get(columnIndex)
																	.getHeader(),
															true, unit);
											if (checkIfList(columnValue)) {
												valueType = ColumnDataType.list;
											}
											// add the type
											table.getColumns().get(columnIndex)
													.addNewDataType(valueType);
											// if it is a unit, add the type of
											// the unit
											if (unit != null
													&& valueType == ColumnDataType.unit)
												table.getColumns()
														.get(columnIndex)
														.addNewUnit(rowNumber,
																unit);

											// use the completle cleaned value
											if (valueType == ColumnDataType.string) {
												TableColumn c = table
														.getColumns().get(
																columnIndex);

												synchronized (c) {
													c.addNewValue(
															rowNumber,
															columnValueNormalized,
															true);
												}
											} else {
												// use simple normalized
												TableColumn c = table
														.getColumns().get(
																columnIndex);

												synchronized (c) {
													c.addNewValue(
															rowNumber,
															simpleStringNormalization(
																	columnValue,
																	true), true);
												}
											}
										}
										columnIndex++;
										i++;
									}
									rowNumber++;
								}
							});

			/*
			 * while ((values = reader.readNext()) != null) { if (rowNumber %
			 * 100 == 0) System.out.println(rowNumber); i = 0; int columnIndex =
			 * 0; ColumnDataType valueType = ColumnDataType.string; for (String
			 * columnValue : values) { // clean the value completely String
			 * columnValueNormalized = StringNormalizer
			 * .clearString(columnValue, true); if
			 * (columnValueNormalized.equals("")) columnValueNormalized =
			 * PipelineConfig.NULL_VALUE .toLowerCase(); if
			 * (!columnValueNormalized .equals(PipelineConfig.NULL_VALUE) &&
			 * !columnValueNormalized .equals(PipelineConfig.NULL_VALUE
			 * .toLowerCase())) { // maybe it is a unit SubUnit unit = new
			 * SubUnit(); String simpleNormalized = simpleStringNormalization(
			 * columnValue, true);
			 * 
			 * // guess the type valueType = typeGuesser
			 * .guessTypeForValue(simpleNormalized, table
			 * .getColumns().get(columnIndex) .getHeader(), true, unit); if
			 * (checkIfList(columnValue)) { valueType = ColumnDataType.list; }
			 * // add the type table.getColumns().get(columnIndex)
			 * .addNewDataType(valueType); // if it is a unit, add the type of
			 * the unit if (unit != null && valueType == ColumnDataType.unit)
			 * table.getColumns().get(columnIndex) .addNewUnit(rowNumber, unit);
			 * } // use the completle cleaned value if (valueType ==
			 * ColumnDataType.string) { table.getColumns() .get(columnIndex)
			 * .addNewValue(rowNumber, columnValueNormalized, true); } else { //
			 * use simple normalized table.getColumns() .get(columnIndex)
			 * .addNewValue( rowNumber, simpleStringNormalization(columnValue,
			 * true), true); } columnIndex++; i++; } rowNumber++; }
			 */
			if (gzip != null)
				gzip.close();
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result = null;
		}
		// set the final types of each column
		for (TableColumn column : table.getColumns())
			column.setFinalDataType();

		return result;
	}

	/**
	 * reads the table but does not do type detection etc.
	 * 
	 * @param tablePath
	 * @param startInstanceRow
	 * @return
	 */
	public Table readTableForSearchFast(String tablePath,
			final int startInstanceRow) {

		// create new table
		final Table table = new Table();
		Table result = table;
		// TODO: take care of the header of the table
		table.setHeader(tablePath);
		// CSVReader reader = null;

		try {
			// create reader
			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(
					tablePath));
			final CSVReader reader = new CSVReader(new InputStreamReader(gzip,
					"UTF-8"));

			// read headers
			String[] columnNames;
			// read the column names
			columnNames = reader.readNext();

			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				column.setHeader(simpleStringNormalization(columnName, false));
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			// int rowNumber = startInstanceRow;

			// String[] values = null;

			for (i = 1; i < startInstanceRow - 1; i++)
				reader.readNext();

			new Parallel<Pair<Integer, String[]>>()
					.producerConsumer(
							new de.mannheim.uni.utils.concurrent.Producer<Pair<Integer, String[]>>() {

								@Override
								public void execute() {
									int row = startInstanceRow;

									String[] values = null;

									try {
										while ((values = reader.readNext()) != null) {
											produce(new Pair<Integer, String[]>(
													row++, values));
										}
									} catch (IOException e) {
										// TODO Auto-generated catch block
										e.printStackTrace();
									}
								}
							}, new Consumer<Pair<Integer, String[]>>() {

								public void execute(
										Pair<Integer, String[]> parameter) {
									int rowNumber = parameter.getFirst();
									String[] values = parameter.getSecond();

									int i = 0;
									int columnIndex = 0;
									// ColumnDataType valueType =
									// ColumnDataType.string;
									for (String columnValue : values) {
										// clean the value completely
										String columnValueNormalized = StringNormalizer
												.clearString(columnValue, true);
										if (columnValueNormalized.equals(""))
											columnValueNormalized = PipelineConfig.NULL_VALUE;

										if (!columnValueNormalized
												.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {

											TableColumn c = table.getColumns()
													.get(columnIndex);

											synchronized (c) {
												c.addNewValue(rowNumber,
														columnValueNormalized,
														true);
											}
										}
										columnIndex++;
										i++;
									}
									rowNumber++;
								}
							});

			gzip.close();
			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			result = null;
		}
		// set the final types of each column
		/*
		 * for (TableColumn column : table.getColumns())
		 * column.setFinalDataType();
		 */

		return result;
	}

	/**
	 * read web table for indexing
	 * 
	 * @param tablePath
	 * @param startInstanceRow
	 * @return
	 */
	public Table readWebTableForIndexing(String tablePath, int startInstanceRow) {
		Table table = null;

		try {
			// create reader
			if (tablePath.endsWith(".gz")) {
				GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(
						tablePath));

				table = readWebTableForIndexing(tablePath, startInstanceRow,
						new InputStreamReader(gzip, "UTF-8"));
			} else if (tablePath.endsWith(".csv")) {
				table = readWebTableForIndexing(tablePath, startInstanceRow,
						new InputStreamReader(new FileInputStream(tablePath),
								"UTF-8"));
			}

		} catch (Exception e) {
			table = null;// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return table;
	}

	public Table readWebTableForIndexing(String tablePath,
			int startInstanceRow, String tableContent) {
		Table table = null;

		try {
			table = readWebTableForIndexing(tablePath, startInstanceRow,
					new StringReader(tableContent));
		} catch (Exception e) {
			table = null;// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return table;
	}

	protected Table readWebTableForIndexing(String tablePath,
			int startInstanceRow, Reader r) {
		// create new table
		final Table table = new Table();
		Table result = table;
		// TODO: take care of the header of the table
		table.setHeader(tablePath);
		// create typeguesser
		final CSVReader reader;

		try {
			// create reader
			if (overrideDelimiter == 0)
				reader = new CSVReader(r);
			else
				reader = new CSVReader(r, overrideDelimiter);

			// read headers
			String[] columnNames;
			// read the column names
			columnNames = reader.readNext();
			while (columnNames != null && areAllNulls(columnNames)) {
				columnNames = reader.readNext();
			}

			if (columnNames == null)
				return null;

			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				String header = columnName;
				if (cleanHeader)
					header = cleanWebHeader(simpleStringNormalization(
							columnName, false));
				column.setHeader(header);
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			String[] values;
			final int rowNumber = startInstanceRow;

			for (i = 1; i < startInstanceRow - 1; i++)
				values = reader.readNext();

			new Parallel<Pair<Integer, String[]>>().producerConsumer(
					new Producer<Pair<Integer, String[]>>() {

						@Override
						public void execute() {
							int row = rowNumber;
							String[] values = null;

							try {
								while ((values = reader.readNext()) != null) {
									produce(new Pair<Integer, String[]>(row++,
											values));
								}
							} catch (IOException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}
					}, new Consumer<Pair<Integer, String[]>>() {

						public void execute(Pair<Integer, String[]> parameter) {
							int rowNumber = parameter.getFirst();
							String[] values = parameter.getSecond();

							if (rowNumber % 100 == 0 && !pipeline.isSilent())
								System.out.println(rowNumber);
							// remove quotes
							int i = 0;
							int columnIndex = 0;
							ColumnDataType valueType = ColumnDataType.string;
							for (String columnValue : values) {
								if (columnIndex < table.getColumns().size()) {
									// clean the value completely
									String simpleNormalized = simpleStringNormalization(
											columnValue, true);

									if (!simpleNormalized
											.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {

										// guess the type
										valueType = typeGuesser
												.guessTypeForValue(
														simpleNormalized, null,
														false, null);
										if (checkIfList(columnValue)) {
											valueType = ColumnDataType.list;
										}
										// add the type
										table.getColumns().get(columnIndex)
												.addNewDataType(valueType);

									}
									// put not normalized value

									TableColumn c = table.getColumns().get(
											columnIndex);

									synchronized (c) {
										c.addNewValue(rowNumber, columnValue,
												false);
									}
								}
								columnIndex++;
								i++;
							}
						}
					});

			/*
			 * while ((values = reader.readNext()) != null) { if (rowNumber %
			 * 100 == 0) System.out.println(rowNumber); // remove quotes i = 0;
			 * int columnIndex = 0; ColumnDataType valueType =
			 * ColumnDataType.string; for (String columnValue : values) { //
			 * clean the value completely String simpleNormalized =
			 * simpleStringNormalization( columnValue, true);
			 * 
			 * if (!simpleNormalized.equals(PipelineConfig.NULL_VALUE) &&
			 * !simpleNormalized .equals(PipelineConfig.NULL_VALUE
			 * .toLowerCase())) {
			 * 
			 * // guess the type valueType = typeGuesser.guessTypeForValue(
			 * simpleNormalized, null, false, null); if
			 * (checkIfList(columnValue)) { valueType = ColumnDataType.list; }
			 * // add the type table.getColumns().get(columnIndex)
			 * .addNewDataType(valueType);
			 * 
			 * } // put not normalized value
			 * 
			 * table.getColumns().get(columnIndex) .addNewValue(rowNumber,
			 * columnValue, false);
			 * 
			 * columnIndex++; i++; } rowNumber++; }
			 */
			reader.close();
		} catch (Exception e) {
			result = null;// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}

	public Table readTableFromString(String content, String tableName) {
		// create new table
		Table table = new Table();
		// TODO: take care of the header of the table
		table.setHeader(tableName);

		CSVReader reader = null;

		try {
			// create reader
			reader = new CSVReader(new InputStreamReader(
					IOUtils.toInputStream(content), "UTF-8"));

			// read headers
			String[] columnNames;
			// read the column names
			columnNames = reader.readNext();
			while (columnNames != null && areAllNulls(columnNames)) {
				columnNames = reader.readNext();
			}

			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				column.setHeader(cleanWebHeader(simpleStringNormalization(
						columnName, false)));
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			String[] values;
			int rowNumber = 2;

			for (i = 1; i < 2 - 1; i++)
				values = reader.readNext();
			while ((values = reader.readNext()) != null) {
				if (rowNumber % 100 == 0)
					System.out.println(rowNumber);
				// remove quotes
				i = 0;
				int columnIndex = 0;
				ColumnDataType valueType = ColumnDataType.string;
				for (String columnValue : values) {
					// clean the value completely
					String simpleNormalized = simpleStringNormalization(
							columnValue, true);

					if (!simpleNormalized
							.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {

						// guess the type
						valueType = typeGuesser.guessTypeForValue(
								simpleNormalized, null, false, null);
						if (checkIfList(columnValue)) {
							valueType = ColumnDataType.list;
						}
						// add the type
						table.getColumns().get(columnIndex)
								.addNewDataType(valueType);

					}
					// put not normalized value

					table.getColumns().get(columnIndex)
							.addNewValue(rowNumber, columnValue, false);

					columnIndex++;
					i++;
				}
				rowNumber++;
			}
			reader.close();
		} catch (Exception e) {
			table = null;// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (!isTableValid(table)) {
			pipeline.getLogger().info(tableName + " is not a valid table!!!!");
			return null;
		}
		// set table full name
		table.setFullPath(tableName);
		return table;

	}

	private static boolean checkIfList(String columnValue) {
		if (columnValue.matches("^\\{.+\\|.+\\}$"))
			return true;
		return false;
	}

	// read different types of tables
	public Table readTable(ReadTableType type, String tablePath) {
		long start = System.currentTimeMillis();
		pipeline.getLogger().info("Start to read table " + tablePath);
		Table table = null;
		switch (type) {
		case test:
			table = readWebTableForTesting(tablePath);
			break;
		case web:
			table = readWebTableForIndexing(tablePath,
					pipeline.getInstanceStartIndex());
			break;

		case search:
			table = readTableForSearch(tablePath,
					pipeline.getInstanceStartIndex());
			break;
		case lod:
			table = readLODtableForIndexing(tablePath);
			break;

		default:
			// throw some exception
			break;
		}
		// check validity of table
		if (type != ReadTableType.search) {
			if (!isTableValid(table)) {
				pipeline.getLogger().info(
						tablePath + " is not a valid table!!!!");
				return null;
			}
		}
		// set table full name
		table.setFullPath(tablePath);
		if (type == ReadTableType.search || type == ReadTableType.test) {
			// identify key
			TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(pipeline);
			keyIdentifier.identifyKey(table);
			if (!table.isHasKey())
				return null;
		}
		long end = System.currentTimeMillis();
		pipeline.getLogger().info(
				"Time reading the table  " + tablePath + ": "
						+ ((double) (end - start) / 1000));

		return table;

	}

	private static boolean areAllNulls(String[] vals) {
		for (String val : vals)
			if (!val.equals(""))
				return false;
		return true;
	}

	/**
	 * Read web table
	 * 
	 * @param tablePath
	 * @param startInstanceRow
	 *            absolute index of the first instance
	 * @return
	 */
	public Table readwebTableForStatustics(String tablePath,
			int startInstanceRow) {

		// create new table
		Table table = new Table();
		// TODO: take care of the header of the table
		table.setHeader(tablePath);
		CSVReader reader = null;

		try {
			// create reader

			reader = new CSVReader(new InputStreamReader(new FileInputStream(
					tablePath), "UTF-8"));

			// read headers
			String[] columnNames;
			// read the column names

			columnNames = reader.readNext();
			while (columnNames != null && areAllNulls(columnNames)) {
				columnNames = reader.readNext();
			}
			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				columnName = cleanWebHeader(columnName);
				columnName = StringNormalizer.clearString(columnName, true);
				column.setHeader(columnName);
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				table.getColumns().add(column);
				i++;
			}
			// absolute row number
			int rowNumber = startInstanceRow;

			String[] values = null;

			for (i = 1; i < startInstanceRow - 1; i++)
				values = reader.readNext();
			while ((values = reader.readNext()) != null) {
				if (rowNumber % 100 == 0)
					System.out.println(rowNumber);
				i = 0;
				int columnIndex = 0;
				ColumnDataType valueType = ColumnDataType.string;
				for (String columnValue : values) {
					// clean the value completely
					String columnValueNormalized = StringNormalizer
							.clearString(columnValue, true);
					if (columnValueNormalized.equals(""))
						columnValueNormalized = PipelineConfig.NULL_VALUE;
					if (!columnValueNormalized
							.equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {
						// maybe it is a unit

						String simpleNormalized = simpleStringNormalization(
								columnValue, true);

						// guess the type
						valueType = typeGuesser
								.guessTypeForValue(simpleNormalized, table
										.getColumns().get(columnIndex)
										.getHeader(), false, null);
						if (checkIfList(columnValue)) {
							valueType = ColumnDataType.list;
						}
						// add the type
						table.getColumns().get(columnIndex)
								.addNewDataType(valueType);

					}
					// use the completle cleaned value
					if (valueType == ColumnDataType.string) {
						table.getColumns()
								.get(columnIndex)
								.addNewValue(rowNumber, columnValueNormalized,
										true);
					} else {
						// use simple normalized
						table.getColumns()
								.get(columnIndex)
								.addNewValue(
										rowNumber,
										simpleStringNormalization(columnValue,
												true), true);
					}
					columnIndex++;
					i++;
				}
				rowNumber++;
			}

			reader.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			table = null;
		}
		// set the final types of each column
		for (TableColumn column : table.getColumns())
			column.setFinalDataType();

		TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(pipeline);
		keyIdentifier.identifyKey(table);

		return table;
	}

	/**
	 * Use to read table for wiki lookup
	 * 
	 * @param tablePath
	 * @return
	 */
	public static String readwebTableWikiLookup(String tablePath) {
		String result = "";
		// create new table

		LinkedHashMap<TableColumn, Double> columnWiki = new LinkedHashMap<TableColumn, Double>();
		LinkedHashMap<Integer, Double> columnWikiScores = new LinkedHashMap<Integer, Double>();
		LinkedList<TableColumn> columnsList = new LinkedList<TableColumn>();
		// TODO: take care of the header of the table

		CSVReader reader = null;

		try {
			// create reader

			GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(
					tablePath));
			reader = new CSVReader(new InputStreamReader(gzip, "UTF-8"));

			// read headers
			String[] columnNames;
			// read the column names

			columnNames = reader.readNext();
			while (columnNames != null && areAllNulls(columnNames)) {
				columnNames = reader.readNext();
			}
			int i = 0;
			for (String columnName : columnNames) {
				TableColumn column = new TableColumn();
				columnName = cleanWebHeader(columnName);
				columnName = StringNormalizer.clearString(columnName, true);
				column.setHeader(columnName);
				// TODO take care of the type
				column.setDataType(ColumnDataType.unknown);
				columnsList.add(column);
				columnWikiScores.put(i, 0.0);
				i++;
			}
			// absolute row number
			int rowNumber = 2;

			String[] values = null;

			for (i = 1; i < rowNumber - 1; i++)
				values = reader.readNext();
			while ((values = reader.readNext()) != null) {
				if (rowNumber % 100 == 0)
					System.out.println(rowNumber);

				int columnIndex = 0;

				for (String columnValue : values) {

					// check if the value is a wiki link
					if (columnValue.toLowerCase().contains("wikipedia.org"))

						columnWikiScores.put(columnIndex,
								columnWikiScores.get(columnIndex) + 1);

					columnIndex++;
				}
				rowNumber++;
			}
			// calculate the final scores
			i = 0;

			for (TableColumn col : columnsList) {
				// if more than 30 % of the columns contain wiki links then
				// return them
				double score = columnWikiScores.get(i)
						/ (double) (rowNumber - 2);
				if (score > 0.1) {
					result += tablePath + "\t" + col.getHeader() + "\t" + score
							+ "\n";
					columnWiki.put(col, score);
				}
				i++;
			}

			reader.close();
			gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}

		// return columnWiki;
		if (!result.equals(""))
			result += "\t\n";

		return result;
	}

	/**
	 * Checks if the table coresponds to the requests from the config file
	 * 
	 * @param table
	 * @return
	 */
	private boolean isTableValid(Table table) {

		if (table == null
				|| table.getColumns().size() < pipeline.getMinCols()
				|| table.getColumns().get(0).getValues().size() < pipeline
						.getMinRows())
			return false;
		return true;
	}

	/**
	 * cleans the string from unwanted special characters
	 * 
	 * @param value
	 * @return
	 */
	public static String simpleStringNormalization(String value,
			boolean removeContentInBrackets) {
		try {
			value = org.apache.commons.lang.StringEscapeUtils
					.unescapeJava(value);
			value = value.replace("\"", "");
			value = value.replace("|", " ");
			value = value.replace(",", "");
			value = value.replace("{", "");
			value = value.replace("}", "");
			value = value.replaceAll("\n", "");

			value = value.replace("&nbsp;", " ");
			value = value.replace("&nbsp", " ");
			value = value.replace("nbsp", " ");
			value = value.replaceAll("<.*>", "");
			if (removeContentInBrackets)
				value = value.replaceAll("\\(.*\\)", "");
			if (value.equals(""))
				value = PipelineConfig.NULL_VALUE;
			value = value.toLowerCase();
			value = value.trim();
		} catch (Exception e) {

		}
		return value;
	}

	public static String cleanWebHeader(String columnName) {
		columnName = columnName.replace("&nbsp;", " ");
		columnName = columnName.replace("&nbsp", " ");
		columnName = columnName.replace("nbsp", " ");
		columnName = columnName.replaceAll("<.*>", "");
		columnName = columnName.replaceAll("\\.", "");
		columnName = columnName.replaceAll("\\$", "");
		// clean the values from additional strings
		if (columnName.contains("/"))
			columnName = columnName.substring(0, columnName.indexOf("/"));

		if (columnName.contains("\\"))
			columnName = columnName.substring(0, columnName.indexOf("\\"));

		if (columnName.contains("|"))
			columnName = columnName.substring(0, columnName.indexOf("|"));

		columnName = columnName.trim();

		return columnName;
	}

	public static void main(String[] args) {
		// ConvertFileToTable fr = new ConvertFileToTable();
		// fr.readTableFromCSVforIndexing("Data\\Test\\AcademicJournal.csv");
		System.out.println(cleanWebHeader("rank |sadsa"));
	}
}
