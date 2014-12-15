package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClassifierDatasetGenerator {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// generateDataset(args[0], args[1], args[2]);
		removeFalseValues(args[0], Integer.parseInt(args[1]),
				Integer.parseInt(args[2]), args[3]);
	}

	public static void removeFalseValues(String simallPath,
			int nmOfTotalFalseToStay, int nmOfTotalTrueToStay,
			String resultingFile) {
		try {
			// open resulting file
			File file = new File(resultingFile);
			file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			// read the simall File
			BufferedReader br = null;

			br = new BufferedReader(new FileReader(simallPath));
			String line = "";
			int indexFile = 0;
			while ((line = br.readLine()) != null) {
				indexFile++;
				if (indexFile % 1000000 == 0)
					System.out.println(indexFile);
				if (line.endsWith("false")) {
					if (nmOfTotalFalseToStay == 0)
						continue;
					nmOfTotalFalseToStay--;
				} else {
					if (nmOfTotalTrueToStay == 0)
						continue;
					nmOfTotalTrueToStay--;
				}

				bw.write(line + "\n");

			}
			bw.close();
			br.close();
			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @param simallPath
	 *            simall filepath
	 * @param tuplePath
	 *            tuple file path
	 * @param resultingFile
	 */
	private static void generateDataset(String simallPath, String tuplePath,
			String resultingFile) {
		// List<String> positivePairs = readTuples(tuplePath);
		// System.out.println("Done reading the tuples!!!");

		try {
			// open resulting file
			File file = new File(resultingFile);
			file.createNewFile();
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			// read the simall File
			BufferedReader br = null;

			br = new BufferedReader(new FileReader(simallPath));
			String line = "";
			int indexFile = 0;
			while ((line = br.readLine()) != null) {
				indexFile++;
				if (indexFile % 1000000 == 0)
					System.out.println(indexFile);
				String[] vals = line.split("\t");
				String tupleStr = vals[2] + "|" + vals[4] + "|" + vals[3] + "|"
						+ vals[5];
				String label = "false";
				if (!vals[9].equals("0.0"))// .contains(tupleStr))
					label = "true";
				String newLine = tupleStr;
				for (int i = 6; i < vals.length; i++) {
					newLine += "\t" + vals[i];
				}
				newLine += "\t" + label;
				bw.write(newLine + "\n");

			}
			bw.close();
			br.close();
			System.out.println("Done");

		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	private static List<String> readTuples(String tuplePath) {
		List<String> tuples = new ArrayList<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(tuplePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";
			int indexFile = 0;
			while ((line = br.readLine()) != null) {
				indexFile++;
				if (indexFile % 1000 == 0)
					break;
				System.out.println(indexFile);
				String[] vals = line.split("\t");
				line = vals[2];
				vals = line.split("\\)\\),\\(\\(");
				vals[0] = vals[0].replaceAll("\\(", "").replaceAll("\\)", "")
						.replace("{", "").replace("}", "");
				vals[vals.length - 1] = vals[vals.length - 1]
						.replaceAll("\\(", "").replaceAll("\\)", "")
						.replace("{", "").replace("}", "");

				for (int i = 0; i < vals.length - 1; i++) {
					String firstTuple = vals[i];
					String[] firstVals = firstTuple.split(",");

					for (int j = i + 1; j < vals.length; j++) {
						String secondTuple = vals[j];
						String[] secondVals = secondTuple.split(",");
						String tupleStr = firstVals[0] + "|" + firstVals[1]
								+ "|" + secondVals[0] + "|" + secondVals[1];
						String invertTupleStr = secondVals[0] + "|"
								+ secondVals[1] + "|" + firstVals[0] + "|"
								+ firstVals[1];
						if (!tuples.contains(invertTupleStr))
							tuples.add(invertTupleStr);
						if (!tuples.contains(tupleStr))
							tuples.add(tupleStr);

					}
				}

			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return tuples;
	}
}
