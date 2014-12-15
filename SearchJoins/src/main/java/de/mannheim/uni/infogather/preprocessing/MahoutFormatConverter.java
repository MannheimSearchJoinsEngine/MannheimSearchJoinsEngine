package de.mannheim.uni.infogather.preprocessing;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.mahout.math.VectorWritable;

import de.mannheim.uni.hadoop.SequenceFileReader;
import de.mannheim.uni.hadoop.SequenceFileWriter;

/***
 * Converts the key produced by mahout's lucene.vector program from LongWritable to IntWritable so mahout's rowsimilarity program can open it ...
 * @author Oliver
 *
 */
public class MahoutFormatConverter {
	
	public static void main(String[] args) throws Exception {
		MahoutFormatConverter mfc = new MahoutFormatConverter();
		
		mfc.convert(args[0], args[1]);
	}
	
	public void convert(String input, String output) throws Exception
	{
		SequenceFileReader<LongWritable, VectorWritable> reader = new SequenceFileReader<LongWritable, VectorWritable>(input, LongWritable.class, VectorWritable.class);
		SequenceFileWriter<IntWritable, VectorWritable> writer = new SequenceFileWriter<IntWritable, VectorWritable>(output, IntWritable.class, VectorWritable.class);
		
		while(reader.next())
		{
			writer.write(new IntWritable((int)reader.getKey().get()), reader.getValue());
		}
	
		reader.close();
		writer.close();
	}
	
}
