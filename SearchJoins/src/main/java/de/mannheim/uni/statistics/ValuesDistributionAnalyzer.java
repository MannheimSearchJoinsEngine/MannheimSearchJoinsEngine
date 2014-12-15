package de.mannheim.uni.statistics;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.FileUtils;

/**
 * @author petar
 * 
 */
public class ValuesDistributionAnalyzer {
	private Map<String, Integer> valuesDist;
	private Map<String, Integer> valueIdDist;

	private Map<String, Integer> propsDist;
	private Map<String, Integer> propsIdDist;

	private Pipeline pipeline;
	ReadTableType type;
	Writer writer;
	ConvertFileToTable ct;

	public static final String delimiter = "\",\"";

	public void makeStatsForRepo(String path, Pipeline pipeline,
			ReadTableType type) {
		this.pipeline = pipeline;
		this.type = type;
		List<String> files = new ArrayList<String>();
		File folder = new File(path);

		files = FileUtils.readFilesFromFolder(path, 80);
		// for (File fileEntry : folder.listFiles()) {
		// files.add(fileEntry.getPath());
		// }
		ct = new ConvertFileToTable(pipeline);
		makeStats(files);
	}

	public void makeStats(List<String> filePaths) {
		long start = System.currentTimeMillis();
		valuesDist = new HashMap<String, Integer>();
		valueIdDist = new HashMap<String, Integer>();
		propsDist = new HashMap<String, Integer>();
		propsIdDist = new HashMap<String, Integer>();

		try {
			File folder = new File(pipeline.getPipelineName() + "Distribution");
			if (folder.canRead() == false) {
				folder.mkdir();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(pipeline.getPipelineName()
							+ "Distribution/" + "general.txt", true), "utf-8"));

		} catch (IOException ex) {
			// report
		}
		if (type == ReadTableType.web) {
			for (String path : filePaths) {
				pipeline.getLogger().info("Analyzing " + path);
				try {
					iterateValuesWeb(path);
				} catch (Exception e) {

				}
			}
		} else {
			for (String path : filePaths) {
				pipeline.getLogger().info("Analyzing " + path);
				iterateValues(path);
			}
		}
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		writeStatstoFile(valuesDist, "valuesDist");
		writeStatstoFile(valueIdDist, "valueIdDist");
		writeStatstoFile(propsDist, "propsDist");
		writeStatstoFile(propsIdDist, "propsIdDist");

		long end = System.currentTimeMillis();

		pipeline.getLogger().info(
				"Time for analyzing all tables: "
						+ ((double) (end - start) / 1000));
	}

	private void writeStatstoFile(Map<String, Integer> map, String name) {
		Writer writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(pipeline.getPipelineName()
							+ "Distribution/" + name + ".txt", true), "utf-8"));
			for (Entry<String, Integer> entry : map.entrySet())
				writer.write(entry.getKey() + "|" + entry.getValue() + "\n");
		} catch (IOException ex) {
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
			}
		}

	}

	public void iterateValuesWeb(String path) {

		Table table = ct.readLODtableForIndexing(path);
		for (TableColumn column : table.getColumns()) {
			String columnName = column.getHeader();
			if (propsDist.containsKey(columnName)) {
				propsDist.put(columnName, propsDist.get(columnName) + 1);
			} else {
				propsDist.put(columnName, 1);
			}
		}
		TableKeyIdentifier kt = new TableKeyIdentifier(pipeline);
		kt.identifyKey(table);

		for (TableColumn column : table.getColumns()) {
			if (column.isKey()) {
				System.out.println(column.getHeader());
				int kl = 0;
				for (String columnValueNormalized : column.getValues().values()) {
					columnValueNormalized = StringNormalizer.clearString(
							columnValueNormalized, true);
					if (valuesDist.containsKey(columnValueNormalized)) {
						valuesDist.put(columnValueNormalized,
								valuesDist.get(columnValueNormalized) + 1);
					} else {
						valuesDist.put(columnValueNormalized, 1);
					}
				}
			}

		}
		try {
			writer.write(path + "\t" + table.getColumns().size() + "\t"
					+ table.getColumns().get(0).getValues().size() + "\n");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public void iterateValues(String path) {
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
			String[] columnFullNames;
			// read the column names
			columnNames = fileLine.split(delimiter);
			// full names
			fileLine = in.readLine();
			columnFullNames = fileLine.split(delimiter);
			if (columnNames.length > 0)
				columnNames[0] = "URI";

			for (int i = 0; i < columnNames.length; i++) {
				String columnName = columnNames[i];
				columnName = columnName.replace("\"", "");
				String fullColumnName = columnFullNames[i];
				fullColumnName = fullColumnName.replace("\"", "");
				if (propsDist.containsKey(columnName)) {
					propsDist.put(columnName, propsDist.get(columnName) + 1);
				} else {
					propsDist.put(columnName, 1);
				}

				if (propsIdDist.containsKey(fullColumnName)) {
					propsIdDist.put(fullColumnName,
							propsIdDist.get(fullColumnName) + 1);
				} else {
					propsIdDist.put(fullColumnName, 1);
				}

			}

			int idIndex = 0;
			int wantedIndex = 0;

			for (int j = 0; j < columnNames.length; j++) {
				if (columnNames[j].toLowerCase().contains("#label")) {
					wantedIndex = j;
					break;
				}
			}
			if (wantedIndex == 0)
				for (int j = 0; j < columnNames.length; j++) {
					if (columnNames[j].toLowerCase().equals("name")
							|| columnNames[j].toLowerCase().equals("name\"")
							|| columnNames[j].toLowerCase().contains("#name")) {
						wantedIndex = j;
						break;
					}
				}

			// skip headers
			fileLine = in.readLine();
			fileLine = in.readLine();
			String[] nextLine;

			int i = 0;
			// the absolute row number in the file
			int rowIndex = 5;
			while ((fileLine = in.readLine()) != null) {
				nextLine = fileLine.split(delimiter);
				String columnValue = nextLine[wantedIndex];
				String columnValueNormalized = ConvertFileToTable
						.simpleStringNormalization(columnValue, true);
				if (valuesDist.containsKey(columnValueNormalized)) {
					valuesDist.put(columnValueNormalized,
							valuesDist.get(columnValueNormalized) + 1);
				} else {
					valuesDist.put(columnValueNormalized, 1);
				}
				String idValue = nextLine[idIndex];
				if (valueIdDist.containsKey(idValue)) {
					valueIdDist.put(idValue, valueIdDist.get(idValue) + 1);
				} else {
					valueIdDist.put(idValue, 1);
				}
				i++;
			}

			writer.write(path + "\t" + columnNames.length + "\t" + i + "\n");
			in.close();
			gzip.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * reads all values, and sums all ocurances, and normalizes the values
	 * 
	 * @param filePath
	 */
	public static void makeFinalStats(String filePath, String name) {
		Map<String, Integer> occuraces = new HashMap<String, Integer>();
		Map<Integer, Integer> normalOccuraces = new HashMap<Integer, Integer>();
		BufferedReader br = null;
		List<String> files = new ArrayList<String>();
		try {
			int nm = 0;

			String sCurrentLine;

			br = new BufferedReader(new FileReader(filePath));

			while ((sCurrentLine = br.readLine()) != null) {
				nm++;
				// System.out.println(sCurrentLine);
				String[] pairs = sCurrentLine.split("\\|");
				String key = pairs[0];
				// key = ConvertFileToTable.simpleStringNormalization(key);
				Integer value = Integer.parseInt(pairs[pairs.length - 1]);
				if (occuraces.containsKey(key)) {
					occuraces.put(key, occuraces.get(key) + value);

				} else {
					occuraces.put(key, value);
				}
				if (nm % 10000 == 0)
					System.out.println(nm);
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}

		// write to file everything
		Writer writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(name + ".txt", true), "utf-8"));
			for (Entry<String, Integer> entry : occuraces.entrySet())
				writer.write(entry.getKey() + "|" + entry.getValue() + "\n");
		} catch (IOException ex) {
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
			}
		}

		// normalize values
		for (Entry<String, Integer> entry : occuraces.entrySet()) {
			if (normalOccuraces.containsKey(entry.getValue())) {
				normalOccuraces.put(entry.getValue(),
						normalOccuraces.get(entry.getValue()) + 1);

			} else
				normalOccuraces.put(entry.getValue(), 1);
		}

		writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(name + "Normal.txt", true), "utf-8"));
			for (Entry<Integer, Integer> entry : normalOccuraces.entrySet())
				writer.write(entry.getKey() + "|" + entry.getValue() + "\n");
		} catch (IOException ex) {
			// report
		} finally {
			try {
				writer.close();
			} catch (Exception ex) {
			}
		}

	}

	public static void main(String[] args) {
		makeFinalStats(args[0], args[1]);
		// System.out.println(System.getProperty("os.arch"));
	}
}
