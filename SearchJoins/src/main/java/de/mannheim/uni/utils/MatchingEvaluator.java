package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Iterator;

import com.martiansoftware.util.StringUtils;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.Matcher;

public class MatchingEvaluator {

	Pipeline pipe;
        enum Decision {Correct, Incorrect, Unknown;}
	
	public MatchingEvaluator(Pipeline pipeline)
	{
		pipe = pipeline;
	}
	
	public static void main(String[] args) throws IOException {
		MatchingEvaluator me = new MatchingEvaluator(Pipeline.getPipelineFromConfigFile("default", "searchjoins.conf"));
		
		me.evaluateMatching(args[0], args[1], args[2]);
	}
	
	public void evaluateMatching(String decided_matching, String fullTable, String output) throws IOException
	{
		System.out.println("Loading full table ...");
		// Load full table
        ConvertFileToTable fileToTable = new ConvertFileToTable(pipe);
        fileToTable.setOverrideDelimiter(';');
        fileToTable.setCleanHeader(false);
        Table t = fileToTable.readWebTableForIndexing(fullTable,2);
		for(TableColumn c : t.getColumns())
		{
			c.setFinalDataType();
		}
		
		// Identify the key
		TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(pipe);
		keyIdentifier.identifyKey(t);
        
        
        System.out.println("Loading matching ...");
        // load matching
        TableObjectsMatch matching = Matcher.readTableObjectsMatch(decided_matching);
        
        // create writer for output
        BufferedWriter w = new BufferedWriter(new FileWriter(output));
        
        System.out.println("Loaded " + matching.getColumns().size() + " matchings.");
        for(ColumnObjectsMatch cm : matching.getColumns())
        {
        	System.out.println("Evaluating matches for " + cm.getColumn1().getHeader());
        	
        	TableColumn col1 = cm.getColumn1();
        	
        	if(col1.getHeader().startsWith("dbpedia_"))
        		col1.setDataSource("dbpedia");
        	
        	int colIndex = 1;
        	for(TableColumn c : cm.getColumn2())
        	{
        		System.out.println("(" + colIndex++ + "/" + cm.getColumn2().size() + ") " +cm.getColumn1().getHeader() + " <-> " + c.getHeader());
        		
        		if(c.getHeader().startsWith("dbpedia_"))
        			c.setDataSource("dbpedia");
        		
        		if(c.getDataSource()==null || c.getDataSource().isEmpty())
        		{
        			System.out.println(c.getHeader() + ": no data source detected, skipping!");
        			continue;
        		}
        		
        		TableColumn dbpCol = null;
        		TableColumn webCol = null;
        		
        		if(col1.getDataSource() !=null && col1.getDataSource().equals("dbpedia"))
        		{
        			dbpCol = col1;
        			webCol = c;
        		}
        		else if(c.getDataSource().equals("dbpedia"))
        		{
        			dbpCol = c;
        			webCol = col1;
        		}
        		else
        		{
        			System.out.println("Illegal data source combination, skipping (col1: " + col1.getDataSource() + ", col2: " + c.getDataSource() + ")");
        			continue;
        		}
        		
        		TableColumn dc1, dc2;
        		dc1 = getColumnFromTable(dbpCol, t);
        		dc2 = getCleanedColumnFromTable(webCol, t);
                        if(c.getHeader().equals("country") || col1.getHeader().equals("country")) {
                            continue;
                        }
        		Decision b = evaluateColumnMatch(dc1, dc2, t.getFirstKey());
        		
        		w.write(dbpCol.getHeader() + "\t" + webCol.getHeader() + "\t" + webCol.getDataSource() + "\t" + b.name() + "\t" + cm.getScore() + "\n");
        	
        	}
        }
        
        w.close();
	}
	
	protected TableColumn getCleanedColumnFromTable(TableColumn c, Table t)
	{
		for(TableColumn tc : t.getColumns())
			if(tc.getHeader().equals(c.getCleaningInfo()))
				return tc;
		return null;
	}
	
	protected TableColumn getColumnFromTable(TableColumn c, Table t)
	{
		for(TableColumn tc : t.getColumns())
			if(tc.getHeader().equals(c.getHeader()) || tc.getHeader().equals(c.getHeader() + "(dbpedia)"))
				return tc;
		return null;
	}
	
	public Decision evaluateColumnMatch(TableColumn dbpedia, TableColumn web, TableColumn key)
	{
		
		int maxKey=0;
		int maxdbp=0;
		for(String v : key.getValues().values())
			maxKey = Math.max(maxKey, v.length());
		
		for(String v : dbpedia.getValues().values())
			maxdbp = Math.max(maxdbp, v.length());
		
		Iterator<Integer> dbpIt = dbpedia.getValues().keySet().iterator();
		int row = 1;
		//while(row < dbpedia.getValues().size())
		while(dbpIt.hasNext())
		{
			System.out.println(StringUtils.padRight("key", Math.max(maxKey - "key".length(),0)) + "\t" 
								+ StringUtils.padRight("dbpedia", Math.max(maxdbp - "dbpedia".length(),0)) + "\t"
								+ "web");
			
			int records=0;
			for(int i=0; i < 10 && dbpIt.hasNext(); i++)
			//for(Integer i : dbpedia.getValues().keySet())
			{
				row = dbpIt.next();
				
				//if(dbpedia.getValues().containsKey(row) && web.getValues().containsKey(row))
				if(web.getValues() != null && web.getValues().containsKey(row))
				{
					if(key.getValues().containsKey(row))
					{
						String v = key.getValues().get(row).trim();
						System.out.print(StringUtils.padRight(v, maxKey - v.length()));
					}
					else
						System.out.print(StringUtils.padRight("", maxKey));
					
					System.out.print("\t");
					
					if(dbpedia.getValues().containsKey(row))
					{
						String v = dbpedia.getValues().get(row).trim();
						System.out.print(StringUtils.padRight(v, maxdbp- v.length()));
					}
					else
						System.out.print(StringUtils.padRight("", maxdbp));
					
					
					System.out.print("\t");
					
					if(web.getValues().containsKey(row))
					{
						System.out.print(web.getValues().get(row).trim());
					}
					
					System.out.print("\n");
					records++;
				}
				else
					i--;
				//row++;
			}
			
			if(records==0)
				return Decision.Incorrect;
			
			try {
				char c = askCommand("Is this a true match? (+:yes -:no x:maybe ?:next 10 records)", new char[] { '+', '-', 'x','?'});
				
				switch(c)
				{
				case '+':
					return Decision.Correct;
				case '-':
					return Decision.Incorrect;
                                case 'x':
					return Decision.Unknown;  
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		System.out.println("No more records.");
		
		try {
			char c = askCommand("Is this a true match? (+:yes -:no x:maybe )", new char[] { '+', '-', 'x'});
			
			switch(c)
			{
			case '+':
				return Decision.Correct;
			case '-':
				return Decision.Incorrect;
			case 'x':
				return Decision.Unknown;  
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return Decision.Unknown;
	}
	

	private char askCommand(String text, char[] commands) throws IOException {
		boolean handled = false;
		char response = '\0';

		BufferedReader reader = new BufferedReader(new InputStreamReader(
				System.in));

		while (!handled) {
			System.out.print(text);

			String line = reader.readLine();

			if (line.trim().length() > 0)
				response = line.trim().charAt(0);

			for (char c : commands) {
				if (c == response)
					handled = true;
			}
		}

		return response;
	}
	
}
