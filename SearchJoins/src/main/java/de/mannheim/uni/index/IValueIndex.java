package de.mannheim.uni.index;

import java.util.List;

import de.mannheim.uni.model.IndexEntry;

public interface IValueIndex {
	List<IndexEntry> getRowValues(String tableName, int rowNm);
	List<IndexEntry> getRowValues(String tableName, int rowNm, List<String> allowedHeaders);
	boolean hasHeader(String tableName, List<String> allowedHeaders);
}
