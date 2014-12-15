package de.mannheim.uni.webtables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import de.mannheim.uni.model.TableStats;

public class FileSet {

	private String URL;
	private String id;
	private File jsonFile;
	private List<File> csvFiles;
	private List<WebTableStats> csvStats;
	private File htmlFile;
	
	public FileSet copy()
	{
		FileSet fs = new FileSet();
		
		fs.id = getId();
		fs.jsonFile = new File(getJsonFile().getAbsolutePath());
		fs.htmlFile = new File(getHtmlFile().getAbsolutePath());
		
		fs.csvFiles = new LinkedList<File>();
		for(File f : getCsvFiles())
			fs.csvFiles.add(new File(f.getAbsolutePath()));
		
		fs.csvStats = new LinkedList<WebTableStats>();
		for(WebTableStats s : getCsvStats())
			fs.csvStats.add(s);
		
		return fs;
	}
	
	public String getURL() {
		return URL;
	}
	
	public List<WebTableStats> getCsvStats() {
		return csvStats;
	}
	
	public File getJsonFile() {
		return jsonFile;
	}
	
	public List<File> getCsvFiles() {
		return csvFiles;
	}
	
	public File getHtmlFile() {
		return htmlFile;
	}
	
	public String getId() {
		return id;
	}
	
	public File getCsvForStat(TableStats stat)
	{
		int tblId = csvStats.indexOf(stat);
		
		// find csv file
		for (File csv : csvFiles) {
			if (csv.getName().matches(id + "\\_" + tblId + "+\\_.+")) {
				return csv;
			}
		}
		
		return null;
	}
	
	protected FileSet()
	{
		
	}
	
	public FileSet(File jsonFile)
	{
		this.jsonFile = jsonFile;
		
		this.id = jsonFile.getName().split("\\_")[0];
		
		File[] csvFiles = jsonFile.getParentFile().listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				return name.matches(id + "\\_.+\\.csv");
			}
		});
		
		if(csvFiles!=null)
		{
			
			this.csvFiles = new ArrayList<File>(csvFiles.length);
			
			for(File f : csvFiles)
				this.csvFiles.add(f);
		}
		
		File[] htmlFile = jsonFile.getParentFile().listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				return name.matches("^" + id + "[^\\.]+$");
			}
		});
		
		if(htmlFile!=null)
		{
			this.htmlFile = htmlFile[0]; 
		}
		
		try {
			this.csvStats = getTableStatsFromJson(this.jsonFile.getAbsolutePath());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static List<FileSet> getFileSets(File folder)
	{
		List<FileSet> list = new LinkedList<FileSet>();
		
		File[] jsonFiles = folder.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});

		if(jsonFiles!=null)
		{
			for (File json : jsonFiles) {
				FileSet fs = new FileSet(json);
				list.add(fs);
			}
		}
		
		return list;
	}
	
	protected List<WebTableStats> getTableStatsFromJson(String filePath)
			throws Exception {
		List<WebTableStats> tableStats = new LinkedList<WebTableStats>();
		JSONParser parser = new JSONParser();
		// read the file
		JSONObject jsonObject = (JSONObject) parser
				.parse(readJsonStringFromFile(filePath));
		String header = (String) jsonObject.get("uri");
		this.URL = header;
		int index=0;
		// read the contentTablesArray
		JSONArray tableStatsArray = (JSONArray) jsonObject.get("contentTables");
		{
			for (Object o : tableStatsArray) {
				JSONObject conTable = (JSONObject) o;
				// switch cols with rows,as there is mistake in the stats files
				double colNm = (Long) conTable.get("csvColCount");
				double rowNm = (Long) conTable.get("rowCount");

				WebTableStats tStats = new WebTableStats();
				tStats.setNmCols(colNm);
				tStats.setNmRows(rowNm);
				tStats.setHeader(csvFiles.get(index++).getAbsolutePath());
				
				long start = (Long) conTable.get("start");
				long end = (Long) conTable.get("end");
				tStats.setTableStart(start);
				tStats.setTableEnd(end);
				
				tableStats.add(tStats);
			}
		}
		return tableStats;

	}
	
	protected String readJsonStringFromFile(String file) {
		String jsonString = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			//
			String line = "";
			while ((line = br.readLine()) != null) {
				jsonString += line.replace(":NaN", "0.0");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jsonString;
	}
	
	public void RemoveTables(List<TableStats> remove)
	{
		for(TableStats s : remove)
		{
			File csv = getCsvForStat(s);
			csvFiles.remove(csv);
			csvStats.remove(s);
		}
	}
	
	public static void main(String[] args) {
		List<FileSet> list = FileSet.getFileSets(new File(args[0]));
		
		for(FileSet fs : list)
		{
			System.out.println(fs.getJsonFile() + "\t" + fs.getHtmlFile() + "\t" + fs.getCsvFiles().size() + " tables");
		}
	}
}
