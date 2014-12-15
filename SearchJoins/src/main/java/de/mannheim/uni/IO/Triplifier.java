package de.mannheim.uni.IO;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

import org.apache.commons.lang.WordUtils;

import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.FileUtils;

public class Triplifier {

	public static final String NT_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>";
	public static final String NT_LABEL = "<http://www.w3.org/2000/01/rdf-schema#label>";
	public static final String NT_DATATYPE_PROPERTY = "<http://www.w3.org/2002/07/owl#DatatypeProperty>";
	public static final String NT_CLASS = "http://www.w3.org/2002/07/owl#Class";

	public static final String SCHEMA_FLOAT = "^^<http://www.w3.org/2001/XMLSchema#float>";
	public static final String SCHEMA_DATE = "^^<http://www.w3.org/2001/XMLSchema#date>";
	public static final String SCHEMA_BOOL = "^^<http://www.w3.org/2001/XMLSchema#boolean>";

	public static final String NT_TABLE_NS = "http://table.searchjoins.de/";

	public static void triplifyTable(String tablePath, String outputPath,
			String namespace) {
		ConvertFileToTable reader = new ConvertFileToTable(
				Pipeline.getPipelineFromConfigFile("ss", "searchJoins.conf"));
		Table table = reader.readTable(ReadTableType.search, tablePath);
		triplifyTable(table, outputPath, namespace);

	}

	public static void triplifyTable(Table table, String outputPath,
			String namespace) {
		TableColumn keyColumn = table.getColumns().get(0);
		if (keyColumn == null) {
			System.out.println("The table doesn't contain a key column!!");
			return;
		}

		// holds all triples
		List<String> allTriples = new ArrayList<String>();

		// we assume that the keys are of type as the column header
		String className = NT_TABLE_NS
				+ WordUtils.capitalize(keyColumn.getHeader()).replace(" ", "");

		for (Entry<Integer, String> key : keyColumn.getValues().entrySet()) {
			if (key.getValue().equals("string"))
				continue;
			// create the object
			String keyUri = NT_TABLE_NS
					+ WordUtils.capitalize(key.getValue()).replace(" ", "");

			// add the class and the label
			allTriples.add(makeTriple(keyUri, NT_TYPE, className,
					ColumnDataType.string));
			allTriples.add(makeTriple(keyUri, NT_LABEL, key.getValue(),
					ColumnDataType.string));

			// iterate all columns and add them in a triple with the key
			for (TableColumn objectColumn : table.getColumns()) {
				if (objectColumn.isKey())
					continue;

				String objectValue = objectColumn.getValues().get(key.getKey());

				allTriples.add(makeTriple(
						keyUri,
						NT_TABLE_NS
								+ CSV2HTMLconvertor
										.getHumanHeader(
												objectColumn.getHeader())
										.replace(" ", "")
										.replaceAll("\\(.*\\)", ""),
						objectValue, objectColumn.getDataType()));
			}
		}

		// add the class
		allTriples.add(makeTriple(className, NT_TYPE, NT_CLASS,
				ColumnDataType.string));
		allTriples.add(makeTriple(className, NT_LABEL,
				WordUtils.capitalize(keyColumn.getHeader()),
				ColumnDataType.string));

		// add the labels and the types of the columns (properties)
		for (TableColumn objectColumn : table.getColumns()) {
			if (objectColumn.isKey())
				continue;
			String frac = WordUtils.capitalize(objectColumn.getHeader())
					.replace(" ", "");
			allTriples.add(makeTriple(NT_TABLE_NS + frac, NT_TYPE,
					NT_DATATYPE_PROPERTY, ColumnDataType.string));
			allTriples.add(makeTriple(NT_TABLE_NS + frac, NT_LABEL,
					WordUtils.capitalize(objectColumn.getHeader()),
					ColumnDataType.string));
		}

		try {
			org.apache.commons.io.FileUtils.writeLines(new File(outputPath),
					allTriples);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String makeTriple(String subject, String predicate,
			String object, ColumnDataType columnDataType) {
		String triple = "";
		try {
			triple = addBrackets(subject, columnDataType) + " "
					+ addBrackets(predicate, columnDataType) + " "
					+ addBrackets(object, columnDataType) + " .";
		} catch (Exception e) {
			return null;
		}
		return triple;
	}

	public static String addBrackets(String str, ColumnDataType columnDataType) {
		if (str == null)
			return null;
		if (str.contains("http")) {
			if (!str.startsWith("<"))
				str = "<" + str;
			if (!str.endsWith(">"))
				str = str + ">";
		} else {
			if (!str.startsWith("\"")) {
				str = WordUtils.capitalize(str);
				str = "\"" + str;
			}
			if (!str.endsWith("\""))
				str = str + "\"";
			if (columnDataType == columnDataType.string
					|| columnDataType == columnDataType.list
					|| columnDataType == columnDataType.link) {
				if (!str.endsWith("@en"))
					str = str + "@en";
			} else if (columnDataType == columnDataType.numeric
					|| columnDataType == columnDataType.coordinate
					|| columnDataType == ColumnDataType.unit)
				str = str + SCHEMA_FLOAT;
			else if (columnDataType == columnDataType.date)
				str = str + SCHEMA_DATE;
			else if (columnDataType == columnDataType.bool)
				str = str + SCHEMA_BOOL;
		}
		return str;
	}

	public static void main(String[] args) {

		triplifyTable("Cities.csv", "Cities.nt", "");
	}

}
