package de.mannheim.uni.hadoop;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class SequenceFileKeyCompressor {

	public void compressKeys(String[] inputFile, String[] outputFile, String outputIndex) throws Exception
	{
		long id = 0;

		if(inputFile.length!=outputFile.length)
			return;
		
		BufferedWriter indexWriter = new BufferedWriter(new FileWriter(outputIndex));
		
		HashMap<String, Long> keys = new HashMap<String, Long>();
		
		for(int i=0;i<inputFile.length;i++)
		{
			TextSequenceFileReader reader = new TextSequenceFileReader(inputFile[i]);
			SequenceFileWriter<LongWritable, Text> writer = new SequenceFileWriter<LongWritable, Text>(outputFile[i], LongWritable.class, Text.class);
			
			while(reader.next())
			{
				long currentId = 0;
				if(keys.containsKey(reader.getKey())) // HTML file may contain duplicate URLs!
					// if its the same key, we don't want the data again!
					; //currentId = keys.get(reader.getKey());
				else
				{
					currentId = id++;
					keys.put(reader.getKey(), currentId);
					indexWriter.write(reader.getKey() + "\t" + currentId + "\n");
					
					writer.write(new LongWritable(currentId), new Text(reader.getValue()));
				}
				
				
			}
			
			reader.close();
			writer.close();
		}
		
		indexWriter.close();
	}

	public static void main(String[] args) throws Exception {
		SequenceFileKeyCompressor s = new SequenceFileKeyCompressor();
		
		if(args.length==0)
		{
			s.compressKeys(new String[] { "CSV" }, new String[] { "CSV_compressed" }, "file_index");
			s.compressKeys(new String[] { "HTML" }, new String[] { "HTML_compressed" }, "url_index");
		}
		else
		{
			File dir = new File(args[0]);
			
			List<String> csvs = new LinkedList<String>();
			List<String> htmls = new LinkedList<String>();
			
			final String prefix = args[1];
			
			for(String f : dir.list(new FilenameFilter() {
				
				public boolean accept(File dir, String name) {
					return name.startsWith(prefix) && name.endsWith("CSV");
				}
			}))
			{
				csvs.add(new File(dir,f).getAbsolutePath());
			}
			
			for(String f : dir.list(new FilenameFilter() {
				
				public boolean accept(File dir, String name) {
					return name.startsWith(prefix) && name.endsWith("HTML");
				}
			}))
			{	
				htmls.add(new File(dir,f).getAbsolutePath());
			}
			
			String[] csv_input = csvs.toArray(new String[0]);
			String[] csv_output = csvs.toArray(new String[0]);
			String[] html_input = htmls.toArray(new String[0]);
			String[] html_output = htmls.toArray(new String[0]);
			
			for(int i=0;i<csv_output.length;i++)
				csv_output[i] += "_compressed";
			
			for(int i=0;i<html_output.length;i++)
				html_output[i] += "_compressed";
			
			s.compressKeys(csv_input, csv_output, new File(dir,"file_index").getAbsolutePath());
			s.compressKeys(html_input, html_output, new File(dir, "url_index").getAbsolutePath());
			
		}
	}
}
