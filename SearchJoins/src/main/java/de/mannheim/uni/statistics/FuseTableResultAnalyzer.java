package de.mannheim.uni.statistics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.LinkedList;
import java.util.List;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.instance.InstanceBasedColumnComparer;

public class FuseTableResultAnalyzer {

	public void analyzeResult(Table fusedTable, Table referenceTable, Pipeline p) {
		Timer t = new Timer("Analyse DataFusion result");
		int rows = fusedTable.getColumns().get(0).getTotalSize() + 2;
		int cols = fusedTable.getColumns().size();
		double[][] similarities = new double[rows][cols];
		double[][] deviation = new double[rows][cols];
		String[][] refValues = new String[rows][cols];
		List<Integer> checkedColumns = new LinkedList<Integer>();

		int col = 0;
		// for each fused column
		for (TableColumn fusedCol : fusedTable.getColumns()) {
			if (!fusedCol.isKey()) {
				// check if the header contains a column name of the reference
				// table
				for (TableColumn refCol : referenceTable.getColumns()) {
					if (fusedCol.getHeader().contains(refCol.getHeader())
							&& !refCol.isKey()) {

						// and calculate the deviation of the values to the
						// values in the reference column
						InstanceBasedColumnComparer cc = new InstanceBasedColumnComparer(p);

						cc.setColumn1(refCol);
						cc.setColumn2(fusedCol);
						cc.run();

						for (int row = 2; row < rows; row++) {
							similarities[row][col] = cc.getResults()
									.getValues().get(row);
							refValues[row][col] = refCol.getValues().get(row);
						}

						cc.setUseDeviation(true);
						cc.run();
						for (int row = 2; row < rows; row++) {
							deviation[row][col] = cc.getResults().getValues()
									.get(row);
						}

						checkedColumns.add(col);

						// a fused column can only be checked against one
						// reference column
						break;
					}
				}
				col++;
			}
		}

		// Format output
		StringBuilder result = new StringBuilder();

		int keyCol = 0;
		for (int c = 0; c < fusedTable.getColumns().size(); c++) {
			if (fusedTable.getColumns().get(c).isKey()) {
				keyCol = c;
				break;
			}
		}

		result.append("Row\tKey\tColumn name\tValue\tReference Value\tSimilarity\tDeviation\n");
		for (int row = 2; row < rows; row++) {
			for (int c : checkedColumns) {
				result.append(row
						+ "\t"
						+ fusedTable.getColumns().get(keyCol).getValues()
								.get(row) + "\t"
						+ fusedTable.getColumns().get(c).getHeader() + "(" + c
						+ ")" + "\t"
						+ fusedTable.getColumns().get(c).getValues().get(row)
						+ "\t" + refValues[row][c] + "\t"
						+ similarities[row][c] + "\t" + deviation[row][c]
						+ "\n");
			}
		}

		try {
			BufferedWriter w = new BufferedWriter(new FileWriter(
					"FusedValues_similarities.csv"));
			w.write(result.toString());
			w.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		t.stop();
	}

}
