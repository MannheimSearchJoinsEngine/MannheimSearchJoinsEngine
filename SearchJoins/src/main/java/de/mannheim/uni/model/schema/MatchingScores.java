package de.mannheim.uni.model.schema;

import java.util.HashMap;

import de.mannheim.uni.model.TableColumn;

public class MatchingScores implements java.io.Serializable {
	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> instanceScores;
	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> labelScores;
	public HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> completeInstanceScores;
}
