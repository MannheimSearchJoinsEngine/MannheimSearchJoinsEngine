/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mannheim.uni.conceptMatching;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.Matcher;
import de.mannheim.uni.schemamatching.instance.InstanceBasedMatcher;
import de.mannheim.uni.utils.CsvConverter;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * @author domi
 */
public class DBpediaMatching {

    public static void main(String[] args) throws FileNotFoundException, IOException, InterruptedException {

        Pipeline pipeline = Pipeline.getPipelineFromConfigFile("domi","searchJoins.conf");
//        CsvConverter conver = new CsvConverter();
//        conver.ConvertCsv("Data/EntityTables/ConceptMatch/JoinedTable.csv", "Data/EntityTables/ConceptMatch/ConvertedJoinedTable.csv");
        ConvertFileToTable fileToTable = new ConvertFileToTable(pipeline);
        fileToTable.setOverrideDelimiter(';');
        fileToTable.setCleanHeader(false);
        Table t = fileToTable.readWebTableForIndexing(args[0],2);
        
        for(TableColumn c : t.getColumns())
        {
        	c.setFinalDataType();
        	if(c.getHeader().startsWith("dbpedia"))
        	{
        		c.setDataSource("dbpedia");
//        		
//        		if(c.getHeader().equals("dbpedia_populationestimate"))
//        		{
//        			for(String v : c.getValues().values())
//        				System.out.println(v);
//        		}
        	}
        	else
        		c.setDataSource("web");
        }
        
		// Identify the key
		TableKeyIdentifier keyIdentifier = new TableKeyIdentifier(pipeline);
		keyIdentifier.identifyKey(t);
        
        Matcher m = new Matcher(pipeline);
        HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> result = m.matchInstances(t);
        TableObjectsMatch matches = m.decideInstanceMatching(result, t);
        m.printTableObjectsMatch(matches, "matching_decided.csv");
        
        File f = new File("matched.csv");
        BufferedWriter write = new BufferedWriter(new FileWriter(f));
        for(ColumnObjectsMatch cm : matches.getColumns()) {
        	write.append(cm.getColumn1().getHeader() + ";");
            for(TableColumn tc : cm.getColumn2()) {
                write.append(tc.getHeader()+";");
            }
            write.append("\n");
        }
        t.writeTableToFile("normalizedtable");
        write.flush();
        write.close();
    }
}
