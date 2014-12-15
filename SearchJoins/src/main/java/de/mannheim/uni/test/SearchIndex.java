package de.mannheim.uni.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.List;

import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.pipelines.Pipeline;

public class SearchIndex {
	public static void main(String[] args) {
		Pipeline pipe = new Pipeline("name",
				args[0]);
		IndexManager ma = new IndexManager(pipe);
		
		// Search all columns matching the query key column
		//model
		System.out.println("Searching ...");
		List<IndexEntry> all = ma.searchAllValues(args[1]);
		
		System.out.println("Writing ...");
		try {
			CSVWriter writer = new CSVWriter(new OutputStreamWriter(new FileOutputStream(
					args[2]), "UTF-8"));
			
			String[] headers = new String[] {
				"Table",
				"Column",
				"Type",
				"Key",
				"Cardinality",
				"Value"
			};
			
			writer.writeNext(headers);
			
			String[] values = new String[headers.length];
			
			for(IndexEntry ie : all)
			{
				values[0] = ie.getTabelHeader();
				values[1] = ie.getColumnHeader();
				values[2] = ie.getColumnDataType();
				values[3] = Boolean.toString(ie.isPrimaryKey());
				values[4] = Integer.toString(ie.getTableCardinality());
				values[5] = ie.getValue();
				writer.writeNext(values);
			}
			
			writer.close();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
