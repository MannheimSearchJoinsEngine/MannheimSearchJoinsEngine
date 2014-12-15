package de.mannheim.uni.infogather.preprocessing;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.hadoop.SequenceFileReader;
import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;

public class SequenceFileTableReader {

	private SequenceFileReader<LongWritable, Text> reader;
	private Pipeline pipeline;
	
	public SequenceFileTableReader(String file, Pipeline p) throws Exception
	{
		reader = new SequenceFileReader<LongWritable, Text>(file, LongWritable.class, Text.class);
		pipeline = p;
		pipeline.setKeyUniqueness(0.6);
	}
	
	public Pair<Long, String> read() throws InstantiationException, IllegalAccessException, IOException
	{
		if(reader.next())
		{
			//System.out.println("Accessing sequence file");
			long keyValue = ((LongWritable)reader.getKey()).get(); 
			String csv = ((Text)reader.getValue()).toString();
			
			return new Pair<Long, String>(keyValue, csv);
		}
		else
			return null;
	}
		
	public void close() throws IOException
	{
		reader.close();
	}
}
