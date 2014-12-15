package de.mannheim.uni.webtables;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import de.mannheim.uni.model.TableStats;

public class WebTableStats
	extends TableStats
{

	private long tableStart;
	private long tableEnd;
	
	public long getTableStart() {
		return tableStart;
	}
	public void setTableStart(long tableStart) {
		this.tableStart = tableStart;
	}
	public long getTableEnd() {
		return tableEnd;
	}
	public void setTableEnd(long tableEnd) {
		this.tableEnd = tableEnd;
	}
	
	
}
