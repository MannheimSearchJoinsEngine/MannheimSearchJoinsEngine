package de.mannheim.uni.statistics;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;

public class NullValuesCounter {

	public static void main(String[] args) {
		double totalNmCols = 0;
		double totalNmRows = 0;
		double totalNmNulls = 0;
		double totalNMtruples = 0;

		double minRows = Integer.MAX_VALUE;
		double maxRows = Integer.MIN_VALUE;

		double minCols = Integer.MAX_VALUE;
		double maxCols = Integer.MIN_VALUE;

		double totalDocs = 0;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("microIndexLogger.log"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// skip the header
		String line = "";
		try {
			while ((line = br.readLine()) != null) {
				try {
					if (!line.contains("|"))
						continue;
					String[] vals = line.split("\\|");
					totalDocs++;
					double rNm = Double.parseDouble(vals[2]);
					double cNm = Double.parseDouble(vals[1]);
					totalNmCols += cNm;
					totalNmRows += rNm;
					totalNmNulls += Double.parseDouble(vals[3]);
					totalNMtruples += (cNm - 1) * rNm;
					if (rNm < minRows)
						minRows = rNm;
					if (rNm > maxRows)
						maxRows = rNm;

					if (cNm < minCols)
						minCols = cNm;
					if (cNm > maxCols)
						maxCols = cNm;

				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.out.println(totalDocs);
		System.out.println(totalNmCols);
		System.out.println((int) totalNmRows);
		System.out.println((int) totalNmNulls);
		System.out
				.println((double) (totalNmNulls / (totalNmCols * totalNmRows)));

		System.out.println((int) minCols);
		System.out.println((int) maxCols);
		System.out.println(totalNmCols / totalDocs);

		System.out.println((int) minRows);
		System.out.println((int) maxRows);

		System.out.println(totalNmRows / totalDocs);

		System.out.println(totalNMtruples);

	}
}
