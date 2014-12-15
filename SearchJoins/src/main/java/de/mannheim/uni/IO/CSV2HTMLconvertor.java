package de.mannheim.uni.IO;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import au.com.bytecode.opencsv.CSVReader;

public class CSV2HTMLconvertor {

	public static final String FIRST_LINE = "<table id=\"table1\" class=\"table table-striped table-bordered\" cellspacing=\"0\" width=\"100%\"><thead>";

	public static final String PLUS_SIGN = "&nbsp;&nbsp;<span class=\"glyphicon glyphicon-plus-sign\"> </span>";

	public static void main(String[] args) {

		generateHTMLTables("Countries.csv", "tmpTables");
	}

	static String infoDiv = "<div id =\"infoDiv\" style=\"display: none;\">";

	static Map<String, Integer> stats = new HashMap<String, Integer>();

	/**
	 * @param inputFile
	 *            the csv file to be converted
	 * @param outputPath
	 *            the output folder path
	 */
	public static void generateHTMLTables(String inputFile, String outputPath) {

		List<String> firstTableLines = new LinkedList<String>();
		firstTableLines.add(FIRST_LINE);

		List<String> secondTable = new LinkedList<String>();
		secondTable.add(FIRST_LINE.replace("table1", "table2"));

		CSVReader reader = null;

		try {
			reader = new CSVReader(new InputStreamReader(new FileInputStream(
					inputFile), "UTF-8"));
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// read headers
		String[] columnNames;
		// read the column names
		try {
			columnNames = reader.readNext();
			firstTableLines.add("<tr>\n<th>"
					+ getHumanHeader(ConvertFileToTable
							.simpleStringNormalization(columnNames[0], true))
					+ "</th>\n</tr>\n");
			stats.put("Cols", columnNames.length);
			secondTable.add(generateHeaders(columnNames, true));

			// read the types
			columnNames = reader.readNext();
			secondTable.add(generateHeaders(columnNames, false));

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		secondTable.add("</thead>\n");
		firstTableLines.add("</thead>\n");
		firstTableLines.add("<tbody>");
		secondTable.add("<tbody>");
		int rowNm = 1;
		try {

			while ((columnNames = reader.readNext()) != null) {
				secondTable.add(generateCells(columnNames));
				firstTableLines.add("<tr>\n<td>\n" + columnNames[0]
						+ "\n</td>\n</tr>\n");
				rowNm++;
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		stats.put("Rows", rowNm - 1);
		firstTableLines.add("</tbody>\n</table>\n");
		secondTable.add("</tbody>\n</table>\n");
		secondTable.add(infoDiv + "</div>");

		try {
			org.apache.commons.io.FileUtils.writeLines(new File(outputPath
					+ "/"
					+ inputFile.substring(inputFile.lastIndexOf("/") + 1)
							.replace(".csv", "") + "Table1.txt"),
					firstTableLines);
			org.apache.commons.io.FileUtils.writeLines(new File(outputPath
					+ "/"
					+ inputFile.substring(inputFile.lastIndexOf("/") + 1)
							.replace(".csv", "") + "Table2.txt"), secondTable);
			writeStatstoFile(
					stats,
					outputPath
							+ "/"
							+ inputFile.substring(
									inputFile.lastIndexOf("/") + 1).replace(
									".csv", "") + "Stats.txt");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	private static String generateCells(String[] columnNames) {

		String cells = "<tr>";
		for (String colName : columnNames) {
			cells += "<td>" + shortenString(colName) + "</td>\n";
		}
		cells += "</tr>\n";
		return cells;
	}

	private static String generateHeaders(String[] columnNames,
			boolean firstHeader) {
		String headers = "<tr>";
		int i = 1;
		boolean isFirst = true;
		for (String colName : columnNames) {
			if (firstHeader) {
				colName = getFormatedHeader(colName, i);
				if (isFirst) {
					headers += "<th id=\"" + i + "\">" + colName + "</th>\n";
					isFirst = false;

				} else {
					headers += "<th id=\"" + i + "\">" + colName + PLUS_SIGN
							+ "</th>\n";
				}
			} else {
				colName = shortenString(colName);
				headers += "<th id=\"" + i + "\">" + colName + "</th>\n";
			}

			i++;
		}
		headers += "</tr>\n";
		return headers;
	}

	private static String getFormatedHeader(String colName, int id) {

		String newHeader = colName;

		int nmSources = 0;
		String divSource = "";
		if (newHeader.contains("(") && newHeader.contains(")")) {
			String nmSourcesStr = newHeader.substring(
					newHeader.lastIndexOf("(") + 1, newHeader.length() - 1);
			nmSources = nmSourcesStr.split("\\|\\|").length;
			newHeader = newHeader.substring(0, newHeader.lastIndexOf("("));
			divSource = getSourceInfoDivFromString(nmSourcesStr, id);
			if (newHeader.contains("(")) {
				try {
					nmSourcesStr = newHeader.substring(
							newHeader.lastIndexOf("(") + 1,
							newHeader.length() - 1);
					nmSources = nmSourcesStr.split("\\|\\|").length;
					newHeader = newHeader.substring(0,
							newHeader.lastIndexOf("("));
					divSource = getSourceInfoDivFromString(nmSourcesStr, id);
				} catch (Exception e) {

				}
			}
			newHeader = getHumanHeader(newHeader);

			newHeader = shortenString(newHeader) + " (" + nmSources
					+ " Sources)";

		} else {
			newHeader = shortenString(getHumanHeader(newHeader));
		}
		infoDiv += divSource + "\n";

		return newHeader;

	}

	public static String getSourceInfoDivFromString(String sourcesStr, int id) {
		String div = "<div id=\"div" + id + "\">" + "\n";
		String sources[] = sourcesStr.split("\\|\\|");

		int webSources = 0;
		List<String> wikiSources = new LinkedList<String>();
		List<String> BTCsources = new LinkedList<String>();
		List<String> MicroSources = new LinkedList<String>();

		for (String src : sources) {
			if (src.contains("datasets/wikitable"))
				wikiSources.add(getFileName(src));
			else if (src.contains("datasets/BTC")) {
				BTCsources.add(getFileName(src));
			} else if (src.contains("datasets/micro")) {
				MicroSources.add(getFileName(src));
			} else if (src.contains("/SearchJoins/Indexes/")) {
				webSources++;
			}

		}
		// add the web tables
		if (webSources > 0) {
			div += "<h5> # Web Tables:</h5> " + webSources + "\n";
			int nm = webSources;
			if (stats.containsKey("#Web Tables"))
				nm += stats.get("#Web Tables");
			stats.put("#Web Tables", nm);
		}
		if (BTCsources.size() > 0) {
			int nm = BTCsources.size();
			if (stats.containsKey("#BTC Tables"))
				nm += stats.get("#BTC Tables");
			stats.put("#BTC Tables", nm);
			div = addListToDiv(BTCsources, "<h5>BTC sources: </h5>\n", div);
		}
		if (MicroSources.size() > 0) {
			int nm = MicroSources.size();
			if (stats.containsKey("#Micro Tables"))
				nm += stats.get("#Micro Tables");
			stats.put("#Micro Tables", nm);
			div = addListToDiv(MicroSources, "<h5>Microdata sources: </h5>\n",
					div);
		}
		if (wikiSources.size() > 0) {
			int nm = wikiSources.size();
			if (stats.containsKey("#Wiki Tables"))
				nm += stats.get("#Wiki Tables");
			stats.put("#Wiki Tables", nm);
			div = addListToDiv(wikiSources, "<h5>Wiki sources: </h5>\n", div);
		}

		return div + "</div>";
	}

	private static String addListToDiv(List<String> bTCsources, String source,
			String div) {
		div += source;
		div += "<ul style=\"list-style-type:disc\">\n";
		for (String st : bTCsources) {
			div += "<li>" + st + "</li>\n";

		}
		div += "</ul>";
		return div;

	}

	public static String getFileName(String fullName) {
		if (fullName.contains("/"))
			fullName = fullName.substring(fullName.lastIndexOf("/") + 1,
					fullName.length());
		return fullName.replace(".csv", "").replace(".gz", "")
				.replace("BTCdatasets_", "");
	}

	public static String getHumanHeader(String header) {
		String newHeader = header;
		if (newHeader.contains("|")) {
			String allHeaders[] = newHeader.split("\\|\\|");
			Map<String, Double> headerMap = new HashMap<String, Double>();

			for (String str : allHeaders) {
				double n = 1;
				if (headerMap.containsKey(str))
					n += headerMap.get(str);
				headerMap.put(str, n);
			}
			headerMap = TableKeyIdentifier.sortMap(headerMap);
			newHeader = headerMap.keySet().iterator().next();
		}

		if (newHeader.contains("#"))
			newHeader = newHeader.substring(newHeader.lastIndexOf("#") + 1,
					newHeader.length());
		return newHeader;
	}

	private static String shortenString(String value) {
		if (value.length() > 40)
			value = value.substring(0, 40) + " ...";
		return value;

	}

	private static void writeStatstoFile(Map<String, Integer> map, String name) {
		Writer writer = null;

		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(name, true), "utf-8"));
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

}
