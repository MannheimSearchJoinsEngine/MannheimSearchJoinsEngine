package de.mannheim.uni.test;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.datafusion.TableDataCleaner;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;

public class FusionTest {
	public static void main(String[] args) {
		// int s = Integer.parseInt("8 900 299");
		// System.out.println(s);
		testFusion(args[0], args[1]);
	}

	public static void testFusion(String tableName, String confFile) {
		Pipeline pipe = Pipeline.getPipelineFromConfigFile("testFusion",
				confFile);
		ConvertFileToTable reader = new ConvertFileToTable(pipe);

		Table table = reader.readTable(ReadTableType.test, tableName);
		table.writeColumnList("fulltable.columns.csv");
		TableDataCleaner dataCleaner = new TableDataCleaner(table, pipe);
		table = dataCleaner.cleanTable();
		// write the table to CSV
		table.writeTableToFile("AugmentedTable");
		table.writeColumnList("fusedtable.columns.csv");
	}
}
