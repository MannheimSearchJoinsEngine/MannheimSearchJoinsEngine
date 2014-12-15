package de.mannheim.uni.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import com.google.common.net.InternetDomainName;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */

public class TableStatsAnalyzer {
	public static enum FunctionType {
		general, value, headers, valuesWeb, generalWeb, headersWeb, headersLenght, domainCount
	};

	public static void main(String[] args) {
		// String path =
		// "/home/pristosk/SearchJoins/Indexing/../../LOD2Tables/BTC/OutputOld/cleanNamespaces_xmlns.com_http___www.w3.org_1999_02_22-rdf-syntax-ns_Property.csv.gz";
		// System.out
		// .println(path.substring(path.indexOf("cleanNamespaces_") + 16,
		// path.indexOf("_http___")));
		// String value = "label born in label";
		// System.out.println(value.replaceAll("label$", "").trim());

		FunctionType func = FunctionType.valueOf(args[0]);
		String indexPath = args[1];
		switch (func) {
		case general:
			sizeStats(indexPath);
			break;
		case value:
			countValuesFromIndex(indexPath);
			break;
		case headers:
			countHeaders(indexPath);
			break;
		case valuesWeb:
			countValuesFromFile(indexPath);
			break;
		case generalWeb:
			countRowsCols(indexPath);
			break;
		case headersWeb:
			headersStats(indexPath);
			break;
		case headersLenght:
			analyzeHeadersLenght(indexPath);
			break;
		case domainCount:
			countDomains(indexPath);
			break;
		default:
			break;
		}

	}

	private static void countDomains(String path) {

		Map<String, Integer> allDomains = new HashMap<String, Integer>();

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		int counter = 0;
		try {
			gzip = new GZIPInputStream(new FileInputStream(path));
			br = new BufferedReader(new InputStreamReader(gzip));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";

			while ((line = br.readLine()) != null) {
				try {
					counter++;
					String vals[] = line.split("\"\\|\"");// line.split("\t");//
					String value = vals[0].replace("\"", "");
					allDomains.put(getNamespace(value), 1);
					// value =
					// ConvertFileToTable.simpleStringNormalization(value,
					// false);
					if (counter % 1000000 == 0)
						System.out.println(counter);
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		try {
			br.close();
			// gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		writeMapToFile(allDomains, "domains.txt");

		System.out.println("TOTAL NUMBER OF DOMAINS: " + allDomains.size());

	}

	public static String getNamespace(String nextLine) {
		try {

			if (nextLine.startsWith("http")) {
				URL url = new URL(nextLine);
				InternetDomainName domainName = InternetDomainName.from(
						url.getHost()).topPrivateDomain();
				return domainName.toString();
			}

		} catch (Exception e) {
			e.printStackTrace();
			return nextLine;
		}

		return nextLine;
	}

	private static void analyzeHeadersLenght(String path) {
		Map<String, Integer> completeLengthCount = new HashMap<String, Integer>();
		Map<String, Integer> termsCount = new HashMap<String, Integer>();
		Map<String, Integer> yearCount = new HashMap<String, Integer>();

		int fileCounter = 1;
		String currentTable = "";
		List<String> valuesOfCurrentTable = new ArrayList<String>();
		int allTables = 0;

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		int counter = 0;
		try {
			gzip = new GZIPInputStream(new FileInputStream(path));
			br = new BufferedReader(new InputStreamReader(gzip));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";
			Pattern pattern = Pattern.compile("(19|20)[0-9][0-9]");

			while ((line = br.readLine()) != null) {
				try {
					counter++;
					String vals[] = line.split("\"\\|\"");// line.split("\t");//
					String value = vals[0].replace("\"", "");
					// value =
					// ConvertFileToTable.simpleStringNormalization(value,
					// false);
					if (!vals[2].equals(currentTable)) {
						allTables++;
						currentTable = vals[2];
						valuesOfCurrentTable = new ArrayList<String>();
					}
					if (valuesOfCurrentTable.contains(value))
						continue;

					int nm = 1;
					int lenght = value.length();

					if (completeLengthCount.containsKey(Integer
							.toString(lenght)))
						nm += completeLengthCount.get(Integer.toString(lenght));
					completeLengthCount.put(Integer.toString(lenght), (int) nm);
					valuesOfCurrentTable.add(value);

					nm = 1;// Double.parseDouble(vals[1]);
					int nmTerms = value.length()
							- value.replace(" ", "").length() + 1;
					if (termsCount.containsKey(Integer.toString(nmTerms)))
						nm += termsCount.get(Integer.toString(nmTerms));
					termsCount.put(Integer.toString(nmTerms), (int) nm);

					// find year

					Matcher matcher = pattern.matcher(value);
					if (matcher.find()) {
						String yearStr = matcher.group(0);
						nm = 1;// Double.parseDouble(vals[1]);

						if (yearCount.containsKey(yearStr))
							nm += yearCount.get(yearStr);
						yearCount.put(yearStr, (int) nm);
					}

					if (counter % 1000000 == 0)
						System.out.println(counter);
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		try {
			br.close();
			// gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writeMapToFile(completeLengthCount, "headersLenghtCount.txt");
		writeMapToFile(termsCount, "termsCount.txt");
		writeMapToFile(yearCount, "yearCount.txt");
		System.out.println(allTables);

	}

	private static void headersStats(String path) {
		Map<String, Integer> headersCount = new HashMap<String, Integer>();
		Map<String, Integer> typeCount = new HashMap<String, Integer>();

		int fileCounter = 1;
		String currentTable = "";
		List<String> valuesOfCurrentTable = new ArrayList<String>();
		int allTables = 0;

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		int counter = 0;
		try {
			gzip = new GZIPInputStream(new FileInputStream(path));
			br = new BufferedReader(new InputStreamReader(gzip));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";

			while ((line = br.readLine()) != null) {
				try {
					counter++;
					String vals[] = line.split("\"\\|\"");// line.split("\t");//
					String value = vals[0].replace("\"", "");
					// value =
					// ConvertFileToTable.simpleStringNormalization(value,
					// false);
					if (!vals[2].equals(currentTable)) {
						allTables++;
						currentTable = vals[2];
						valuesOfCurrentTable = new ArrayList<String>();
					}
					if (valuesOfCurrentTable.contains(value))
						continue;

					double nm = 1;// Double.parseDouble(vals[1]);
					if (headersCount.containsKey(value))
						nm += headersCount.get(value);
					headersCount.put(value, (int) nm);
					valuesOfCurrentTable.add(value);

					nm = 1;// Double.parseDouble(vals[1]);
					String type = vals[1].replace("\"", "");
					if (typeCount.containsKey(type))
						nm += typeCount.get(type);
					typeCount.put(type, (int) nm);

					if (counter % 1000000 == 0)
						System.out.println(counter);
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		try {
			br.close();
			// gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writeMapToFile(headersCount, "headersCount.txt");
		writeMapToFile(typeCount, "typesCount.txt");
		System.out.println(allTables);
	}

	public static void countRowsCols(String path) {
		Map<String, Integer> colsCount = new HashMap<String, Integer>();
		Map<String, Integer> rowCount = new HashMap<String, Integer>();
		Map<String, Integer> tldCount = new HashMap<String, Integer>();
		Map<String, Integer> langCount = new HashMap<String, Integer>();

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		int counter = 0;
		try {
			gzip = new GZIPInputStream(new FileInputStream(path));
			br = new BufferedReader(new InputStreamReader(gzip));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";

			while ((line = br.readLine()) != null) {
				try {
					counter++;
					String vals[] = line.split("\"\\|\"");
					String cols = vals[2].replace("\"", "");
					String rows = vals[3].replace("\"", "");
					String tld = vals[1].replace("\"", "");
					String lang = vals[4].replace("\"", "");
					// value =
					// ConvertFileToTable.simpleStringNormalization(value,
					// false);
					// if (!vals[1].equals(currentTable)) {
					// currentTable = vals[1];
					// valuesOfCurrentTable = new ArrayList<String>();
					// }
					// if (valuesOfCurrentTable.contains(value))
					// continue;

					double nm = 1;
					if (colsCount.containsKey(cols))
						nm += colsCount.get(cols);
					colsCount.put(cols, (int) nm);

					// add rows
					nm = 1;
					if (rowCount.containsKey(rows))
						nm += rowCount.get(rows);
					rowCount.put(rows, (int) nm);
					// valuesOfCurrentTable.add(value);

					// add tld
					nm = 1;
					if (tldCount.containsKey(tld))
						nm += tldCount.get(tld);
					tldCount.put(tld, (int) nm);

					// add language
					nm = 1;
					if (langCount.containsKey(lang))
						nm += langCount.get(lang);
					langCount.put(lang, (int) nm);

					if (counter % 1000000 == 0)
						System.out.println(counter);
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		try {
			br.close();
			gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writeMapToFile(colsCount, "colsCount.txt");
		writeMapToFile(rowCount, "rowsCount.txt");

		writeMapToFile(tldCount, "tldCount.txt");
		writeMapToFile(langCount, "langCount.txt");
	}

	public static void countValuesFromFile(String path) {
		Map<String, Integer> valuesCount = new HashMap<String, Integer>();
		int fileCounter = 1;
		String currentTable = "";
		List<String> valuesOfCurrentTable = new ArrayList<String>();
		int allTables = 0;

		BufferedReader br = null;
		GZIPInputStream gzip = null;
		int counter = 0;
		try {
			gzip = new GZIPInputStream(new FileInputStream(path));
			br = new BufferedReader(new InputStreamReader(gzip));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";

			while ((line = br.readLine()) != null) {
				try {
					counter++;
					String vals[] = line.split("\"\\|\"");// line.split("\t");//
					String value = vals[0].replace("\"", "");
					// value =
					// ConvertFileToTable.simpleStringNormalization(value,
					// false);
					if (!vals[1].equals(currentTable)) {
						allTables++;
						currentTable = vals[1];
						valuesOfCurrentTable = new ArrayList<String>();
					}
					if (valuesOfCurrentTable.contains(value))
						continue;

					double nm = 1;// Double.parseDouble(vals[1]);
					if (valuesCount.containsKey(value))
						nm += valuesCount.get(value);
					valuesCount.put(value, (int) nm);
					valuesOfCurrentTable.add(value);

					if (counter % 1000000 == 0)
						System.out.println(counter);
					// avoid out of memory
					if (counter > 990000000) {
						writeMapToFile(valuesCount, "valuesCount" + fileCounter
								+ ".txt");
						valuesCount = new HashMap<String, Integer>();
						fileCounter++;
						counter = 0;
					}
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
		} catch (Exception e) {
			// TODO: handle exception
		}
		try {
			br.close();
			// gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		writeMapToFile(valuesCount, "valuesCount" + fileCounter + ".txt");
		System.out.println(allTables);
	}

	public static final String delimiter = "\",\"";

	/**
	 * use for headers statistics
	 * 
	 * @param loggerPath
	 */
	public static void countHeaders(String loggerPath) {
		List<String> tablesList = populateListWithTables(loggerPath, true);
		Map<String, Integer> valuesCount = new HashMap<String, Integer>();
		Map<String, List<String>> tablesNames = new HashMap<String, List<String>>();

		for (String path : tablesList) {
			String tableName = path;
			if (path.contains("DBpedia2Tables"))
				tableName = "DBpedia";
			else if (path.contains("Yago2Tables")) {
				tableName = "YAGO";

			} else if (path.contains("BTC/OutputOld")) {
				try {
					tableName = path.substring(
							path.indexOf("cleanNamespaces_") + 16,
							path.indexOf("_http___"));
				} catch (Exception e) {
					System.out.println(path);
					e.printStackTrace();
				}
			}

			GZIPInputStream gzip = null;
			BufferedReader in = null;
			try {
				gzip = new GZIPInputStream(new FileInputStream(path));

				in = new BufferedReader(new InputStreamReader(gzip));

				// read headers
				String fileLine = in.readLine();
				String[] columnNames;
				// read the column names
				columnNames = fileLine.substring(1, fileLine.length() - 1)
						.split(delimiter);

				for (String value : columnNames) {
					value = StringNormalizer.clearString(value, true);
					if (value.startsWith("has "))
						value = value.replaceFirst("has ", "");
					if (value.contains(" has "))
						value = value.replace(" has ", "");
					if (value.contains(" ahas "))
						value = value.replace(" ahas ", "");

					int nm = 1;
					List<String> colTables = new ArrayList<String>();
					if (valuesCount.containsKey(value)) {
						colTables = tablesNames.get(value);
						if (colTables.contains(tableName))
							continue;

						nm += valuesCount.get(value);
					}
					colTables.add(tableName);
					tablesNames.put(value, colTables);

					valuesCount.put(value, nm);
				}
				// remove ..label props
				List<String> toRemove = new ArrayList<String>();
				for (Entry<String, Integer> entry : valuesCount.entrySet()) {
					String value = entry.getKey();
					if (value.endsWith("label")
							&& valuesCount.containsKey(value.replaceAll(
									"label$", "").trim()))
						toRemove.add(value);
				}
				for (String str : toRemove)
					valuesCount.remove(str);

			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				try {
					gzip.close();
					in.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		}
		writeMapToFile(valuesCount, "HeadersCount");
	}

	/**
	 * use for general statiscits
	 * 
	 * @param loggerPath
	 */
	public static void sizeStats(String loggerPath) {
		List<String> tablesList = populateListWithTables(loggerPath, false);
		Map<String, Integer> rowsCount = new HashMap<String, Integer>();
		Map<String, Integer> colsCount = new HashMap<String, Integer>();

		double totalCols = 0;
		double totalRows = 0;
		double totalNulls = 0;

		double minCols = Double.MAX_VALUE;
		double maxCols = Double.MIN_VALUE;
		double minRows = Double.MAX_VALUE;
		double maxRows = Double.MIN_VALUE;

		double maxNulls = Double.MIN_VALUE;
		double minNulls = Double.MAX_VALUE;

		for (String path : tablesList) {
			String[] vals = path.split("\\|");
			double cols = Double.parseDouble(vals[1]);
			double rows = Double.parseDouble(vals[2]);
			double nulls = Double.parseDouble(vals[3]);
			double avgNulls = Double.parseDouble(vals[4]);

			totalCols += cols;
			totalRows += rows;
			totalNulls += nulls;

			if (cols < minCols)
				minCols = cols;
			if (cols > maxCols)
				maxCols = cols;

			if (rows < minRows)
				minRows = rows;
			if (rows > maxRows)
				maxRows = rows;

			if (avgNulls < minNulls)
				minNulls = avgNulls;
			if (avgNulls > maxNulls && avgNulls < 1)
				maxNulls = avgNulls;

			int nm = 1;
			if (rowsCount.containsKey(Double.toString(rows))) {
				nm += rowsCount.get(Double.toString(rows));
			}
			rowsCount.put(Double.toString(rows), nm);

			nm = 1;
			if (colsCount.containsKey(Double.toString(cols))) {
				nm += colsCount.get(Double.toString(cols));
			}
			colsCount.put(Double.toString(cols), nm);

		}
		Map<String, String> general = new HashMap<String, String>();
		general.put("totalCols", Double.toString(totalCols));
		general.put("totalRows", Double.toString(totalRows));
		general.put("totalNulls", Double.toString(totalNulls));
		general.put("minCols", Double.toString(minCols));
		general.put("minRows", Double.toString(minRows));
		general.put("maxCols", Double.toString(maxCols));
		general.put("maxRows", Double.toString(maxRows));

		general.put("minNulls", Double.toString(minNulls));
		general.put("maxNulls", Double.toString(maxNulls));

		general.put("totalTables", Integer.toString(tablesList.size()));

		writeMapToFile(rowsCount, "rowsCount");
		writeMapToFile(colsCount, "colsCount");
		writeMapToFileString(general, "generalStats");
	}

	/**
	 * use to count values from all indexed files
	 * 
	 * @param loggerPath
	 */
	public static void countValuesFromIndex(String loggerPath) {
		Pipeline pipeline = Pipeline.getPipelineFromConfigFile("statsPipe",
				"searchJoins.conf");
		// populate the list
		List<String> tablesList = populateListWithTables(loggerPath, true);
		Map<String, Integer> valuesCount = new HashMap<String, Integer>();

		Map<String, List<String>> tablesNames = new HashMap<String, List<String>>();

		// for each table count the keys
		for (String table : tablesList) {
			String tableName = table;
			if (table.contains("DBpedia2Tables"))
				tableName = "DBpedia";
			else if (table.contains("Yago2Tables")) {
				tableName = "YAGO";

			} else if (table.contains("BTC/OutputOld")) {
				try {
					tableName = table.substring(
							table.indexOf("cleanNamespaces_") + 16,
							table.indexOf("_http___"));
				} catch (Exception e) {
					System.out.println(table);
					e.printStackTrace();
				}
			}
			try {
				long start = System.currentTimeMillis();
				pipeline.getIndexManager().countValuesFromIndex(valuesCount,
						table, tablesNames, tableName);
				System.out.println(table + " Done: "
						+ +(System.currentTimeMillis() - start));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// write the map to a file

		writeMapToFile(valuesCount, "ValuesCount");
	}

	public static void writeMapToFile(Map<String, Integer> valuesCount,
			String name) {
		// write the map to a file
		Writer writer = null;
		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(name, true), "utf-8"));
			for (Entry<String, Integer> entry : valuesCount.entrySet()) {
				writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	private static void writeMapToFileString(Map<String, String> valuesCount,
			String name) {
		// write the map to a file
		Writer writer = null;
		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(name, false), "utf-8"));
			for (Entry<String, String> entry : valuesCount.entrySet()) {
				writer.write(entry.getKey() + "\t" + entry.getValue() + "\n");
			}
		} catch (IOException ex) {
			ex.printStackTrace();
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	/**
	 * reads the IndexLogger ouptut
	 * 
	 * @return
	 */
	private static List<String> populateListWithTables(String loggerPath,
			boolean onlyFileName) {
		List<String> tablesList = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(loggerPath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			// skip the header
			String line = br.readLine();

			while ((line = br.readLine()) != null) {
				if (line.contains("|")) {
					String fileName = line.split("\\|")[0]
							.replace("INFO: ", "").trim();
					if (onlyFileName) {
						tablesList.add(fileName);
					} else {
						tablesList.add(line.replace("INFO: ", ""));
					}
				}
			}
		} catch (Exception e) {

		}
		return tablesList;
	}

	public static void writeToFile(String path, String folderPath) {
		Writer writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(path, true), "utf-8"));

			writer.write(folderPath + "\n");
		} catch (IOException ex) {
			ex.printStackTrace();
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}
	}

	public static void makeStats(String path, boolean isWebtable) {

		File folder = new File(path);
		ConvertFileToTable fileToTable = new ConvertFileToTable(new Pipeline(
				"Tmp", "Tmp"));
		for (File fileEntry : folder.listFiles()) {
			if (!fileEntry.getPath().endsWith(".gz"))
				continue;
			long start = System.currentTimeMillis();
			System.out.println("Converting " + fileEntry.getPath());
			Table table = null;
			if (!isWebtable)
				table = fileToTable
						.readLODtableForIndexing(fileEntry.getPath());
			else
				table = fileToTable.readWebTableForIndexing(
						fileEntry.getPath(), 2);
			int nullValues = 0;
			String valueEntry = table.getHeader() + "|0|0|0|0|0";
			if (table.getColumns().size() > 0
					&& table.getColumns().get(0).getValues().size() > 0) {
				for (TableColumn col : table.getColumns()) {
					if (col.getValuesInfo().containsKey(
							PipelineConfig.NULL_VALUE))
						nullValues += col.getValuesInfo().get(
								PipelineConfig.NULL_VALUE);
				}
				double frac = (double) ((double) nullValues / (double) (table
						.getColumns().size() * table.getColumns().get(0)
						.getValues().size()));
				valueEntry = table.getHeader() + "|"
						+ table.getColumns().size() + "|"
						+ table.getColumns().get(0).getValues().size() + "|"
						+ nullValues + "|" + frac + "|"
						+ table.getColumns().size();

			}
			writeToFile(path.replaceAll("[^a-zA-Z0-9.-]", "_") + "Stats.txt",
					valueEntry);
			// files.add(fileEntry.getPath());
			long end = System.currentTimeMillis();
			System.out.println("Done " + fileEntry.getPath() + " for "
					+ (end - start));
		}
	}

	// public static void main(String args[]) {
	// String path = "Data/DBpediaTables";
	// boolean isWebTable = false;
	// if (args != null && args.length > 0 && args[0] != null)
	// path = args[0];
	// else {
	// System.out.println("Please provede a path");
	// return;
	// }
	// if (args != null && args.length > 1 && args[1] != null) {
	// isWebTable = Boolean.parseBoolean(args[1]);
	// }
	// makeStats(path, isWebTable);
	// }

}
