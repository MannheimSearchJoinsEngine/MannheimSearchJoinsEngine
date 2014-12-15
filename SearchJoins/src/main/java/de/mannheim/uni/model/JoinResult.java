package de.mannheim.uni.model;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPOutputStream;

import org.apache.lucene.search.TotalHitCountCollector;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;

import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.PipelineFactory.PipelineType;

/**
 * @author petar
 * 
 */
public class JoinResult 
	implements java.io.Serializable
{

	private String header;

	private String leftColumn;

	private String leftTable;

	private String rightColumn;

	private String rightTable;

	private int leftColumnCardinality;

	private int rightColumncardinality;

	private int leftColumnDistinctValues;

	private int rightColumnDistinctValues;

	private int count;

	private int leftSumMultiplicity;

	private int rightSumMultiplicity;

	private int joinSize;

	double totalRank;

	double rank;

	double luceneTotalRank;

	double labelBasedSchemaMatchingRank;

	Map<IndexEntry, IndexEntry> joinPairs;

	private Pipeline.RankingType rankingType;

	public JoinResult() {
		// TODO Auto-generated constructor stub
		rank = 0;
		count = 0;
		leftSumMultiplicity = 0;
		rightSumMultiplicity = 0;
		joinSize = 0;
		leftColumnCardinality = leftColumnDistinctValues = 0;
		rightColumncardinality = rightColumnDistinctValues = 0;
		joinPairs = new HashMap<IndexEntry, IndexEntry>();
		// default
		rankingType = Pipeline.RankingType.entityTableCoverage;
	}

	public void setRankingType(Pipeline.RankingType rankingType) {
		this.rankingType = rankingType;
	}

	public Pipeline.RankingType getRankingType() {
		return rankingType;
	}

	public String getHeader() {
		return header;
	}

	public void setHeader() {
		this.header = leftTable + "|" + leftColumn + "|" + rightTable + "|"
				+ rightColumn;
	}

	public String getLeftColumn() {
		return leftColumn;
	}

	public void setLeftColumn(String leftColumn) {
		this.leftColumn = leftColumn;
	}

	public String getLeftTable() {
		return leftTable;
	}

	public void setLeftTable(String leftTable) {
		this.leftTable = leftTable;
	}

	public String getRightColumn() {
		return rightColumn;
	}

	public void setRightColumn(String rightColumn) {
		this.rightColumn = rightColumn;
	}

	public String getRightTable() {
		return rightTable;
	}

	public void setRightTable(String rightTable) {
		this.rightTable = rightTable;
	}

	public int getLeftColumnCardinality() {
		return leftColumnCardinality;
	}

	public void setLeftColumnCardinality(int leftColumnCardinality) {
		this.leftColumnCardinality = leftColumnCardinality;
	}

	public int getRightColumncardinality() {
		return rightColumncardinality;
	}

	public void setRightColumncardinality(int rightColumncardinality) {
		this.rightColumncardinality = rightColumncardinality;
	}

	public int getLeftColumnDistinctValues() {
		return leftColumnDistinctValues;
	}

	public void setLeftColumnDistinctValues(int leftColumnDistinctValues) {
		this.leftColumnDistinctValues = leftColumnDistinctValues;
	}

	public int getRightColumnDistinctValues() {
		return rightColumnDistinctValues;
	}

	public void setRightColumnDistinctValues(int rightColumnDistinctValues) {
		this.rightColumnDistinctValues = rightColumnDistinctValues;
	}

	public double getLuceneTotalRank() {
		return luceneTotalRank;
	}

	public void setLuceneTotalRank(double luceneTotalRank) {
		this.luceneTotalRank = luceneTotalRank;
	}

	public void setHeader(String header) {
		this.header = header;
	}

	public int getCount() {
		return count;
	}

	public void setCount(int count) {
		this.count = count;
	}

	public int getLeftSumMultiplicity() {
		return leftSumMultiplicity;
	}

	public void setLeftSumMultiplicity(int leftSumMultiplicity) {
		this.leftSumMultiplicity = leftSumMultiplicity;
	}

	public int getRightSumMultiplicity() {
		return rightSumMultiplicity;
	}

	public void setRightSumMultiplicity(int rightSumMultiplicity) {
		this.rightSumMultiplicity = rightSumMultiplicity;
	}

	public int getJoinSize() {
		return joinSize;
	}

	public void setJoinSize(int joinSize) {
		this.joinSize = joinSize;
	}

	public double getRank() {
		return rank;
	}

	public void setRank(double rank) {
		this.rank = rank;
	}

	public Map<IndexEntry, IndexEntry> getJoinPairs() {
		return joinPairs;
	}

	public void setJoinPairs(Map<IndexEntry, IndexEntry> joinPairs) {
		this.joinPairs = joinPairs;
	}

	public double getTotalRank() {
		return totalRank;
	}

	public void setTotalRank(double totalRank) {
		this.totalRank = totalRank;
	}

	public double getLabelBasedSchemaMatchingRank() {
		return labelBasedSchemaMatchingRank;
	}

	public void setLabelBasedSchemaMatchingRank(
			double labelBasedSchemaMatchingRank) {
		this.labelBasedSchemaMatchingRank = labelBasedSchemaMatchingRank;
	}

	public String print() {
		String entityTbalePath = "/home/pristosk/LOD2Tables/Yago2Tables/Output/"
				+ rightTable;
		if (joinPairs.values().iterator().next().getFullTablePath() != null) {
			entityTbalePath = joinPairs.values().iterator().next()
					.getFullTablePath();
			if (entityTbalePath.startsWith("..")) {
				entityTbalePath.replace("../..", "/home/pristosk");
			}
		}

		StringBuilder sb = new StringBuilder();
		sb.append("-----------------------------------------------");
		sb.append(entityTbalePath);
		sb.append("-----------------------------------------------");
		sb.append("\n");
		// System.out.println("-----------------------------------------------");
		// System.out.println("Lucene total rank: " + this.luceneTotalRank);

		// sb.append("Lucene total rank: " + this.luceneTotalRank);
		sb.append("\n");
		// System.out.println(header + "|" + leftColumnCardinality + "|"
		// + rightColumncardinality + "|" + leftColumnDistinctValues + "|"
		// + rightColumnDistinctValues + "|" + count + "|"
		// + leftSumMultiplicity + "|" + rightSumMultiplicity + "|"
		// + joinSize + "||" + rank);
		sb.append(header + "|" + leftColumnCardinality + "|"
				+ rightColumncardinality + "|" + leftColumnDistinctValues + "|"
				+ rightColumnDistinctValues + "|" + count + "|"
				+ leftSumMultiplicity + "|" + rightSumMultiplicity + "|"
				+ joinSize + "||" + totalRank);
		sb.append("\n");
		for (Entry<IndexEntry, IndexEntry> entry : joinPairs.entrySet()) {
			// System.out.println(entry.getKey() + "--->" + entry.getValue());
			// add refining attributes, if any
			String leftAdditionalMatchedValues = " [";
			for (IndexEntry en : entry.getKey().getRefineAttrs()) {
				leftAdditionalMatchedValues += en.getValue() + ",";
			}
			leftAdditionalMatchedValues += "]";
			String rightAdditionalMatchedValues = " [";
			for (IndexEntry en : entry.getValue().getRefineAttrs()) {
				rightAdditionalMatchedValues += en.getValue() + ",";
			}
			rightAdditionalMatchedValues += "]";
			sb.append(entry.getKey().getValue() + leftAdditionalMatchedValues
					+ "--->" + entry.getValue().getValue()
					+ rightAdditionalMatchedValues);
			// sb.append(entry.getKey().getEntryID() + "--->"
			// + entry.getValue().getEntryID());
			sb.append("\n");
		}

		// System.out.println("-----------------------------------------------");
		sb.append("-----------------------------------------------");
		sb.append("\n");
		return sb.toString();

	}

	public static void writeCsv(List<JoinResult> results, String filename)
			throws IOException {
		CSVWriter writer = new CSVWriter(new OutputStreamWriter(
				new FileOutputStream(filename), "UTF-8"));

		String[] values = new String[9];

		for (JoinResult r : results) {
			int i = 0;
			// query table key column
			values[i++] = r.getLeftColumn();
			// number of matches
			values[i++] = Integer.toString(r.getCount());

			// entity table name
			values[i++] = r.getRightTable();
			// entity table key column
			values[i++] = r.getRightColumn();
			// entity table row count
			values[i++] = Integer.toString(r.getRightColumncardinality());
			// entity table path
			values[i] = "/home/pristosk/LOD2Tables/Yago2Tables/Output/";
			if (r.getJoinPairs().values().iterator().hasNext()
					&& r.getJoinPairs().values().iterator().next()
							.getFullTablePath() != null) {
				values[i] = r.getJoinPairs().values().iterator().next()
						.getFullTablePath();
				if (values[i].startsWith("..")) {
					values[i] = values[i].replace("../..", "/home/pristosk");
				}
			}

			for (Entry<IndexEntry, IndexEntry> entry : r.getJoinPairs()
					.entrySet()) {
				// query table key value
				values[i + 1] = entry.getKey().getValue();

				// entity table key value
				values[i + 2] = entry.getValue().getValue();

				// entity table row number
				values[i + 3] = Integer.toString(entry.getValue().getEntryID());

				writer.writeNext(values);
			}
		}

		writer.close();
	}

	public static Map<String, JoinResult> readCsvMap(String filename)
			throws IOException {
		Map<String, JoinResult> result = new HashMap<String, JoinResult>();
		List<JoinResult> list = readCsv(filename);

		for (JoinResult r : list) {
			result.put(r.getRightTable(), r);
		}

		return result;
	}

	public static List<JoinResult> readCsv(String filename) throws IOException {
		List<JoinResult> result = new ArrayList<JoinResult>();

		CSVReader reader = new CSVReader(new InputStreamReader(
				new FileInputStream(filename), "UTF-8"));

		String[] values;

		JoinResult r = null;

		int i = 0;
		while ((values = reader.readNext()) != null) {
			if (values.length != 9) // Skip invalid rows
				continue;

			if (r == null || !r.getRightTable().equals(values[2])) {
				// A new JoinResult is starting

				// Add previous result to the list
				if (r != null)
					result.add(r);

				// JoinResult starting
				r = new JoinResult();

				i = 0;
				r.setLeftColumn(values[i++]);
				r.setCount(Integer.parseInt(values[i++]));
				r.setRightTable(values[i++]);
				r.setRightColumn(values[i++]);
				r.setRightColumncardinality(Integer.parseInt(values[i]));
			}

			// row values for current JoinResult
			IndexEntry left, right;
			left = new IndexEntry();
			left.setValue(values[i + 2]);
			right = new IndexEntry();
			right.setFullTablePath(values[i + 1]);
			right.setValue(values[i + 3]);
			right.setEntryID(Integer.parseInt(values[i + 4]));
			r.getJoinPairs().put(left, right);
		}

		// also add last result
		if (r != null)
			result.add(r);

		reader.close();

		return result;
	}
}
