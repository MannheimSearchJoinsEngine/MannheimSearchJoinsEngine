package de.mannheim.uni.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class TextSequenceFileReader {
	private String fileName;
	private String currentKey = null;
	private String currentValue = null;
	private SequenceFile.Reader reader;
	
	public TextSequenceFileReader(String inputFile) throws Exception {
		fileName = inputFile;
		
		File f = new File(fileName);
		if(!f.exists())
			throw new Exception("File does not exist!");
		
		Path path = new Path(fileName);
		LocalSetup local = new LocalSetup();
		
		Configuration conf = local.getConf();
		FileSystem fs = local.getLocalFileSystem();
		
		reader = new SequenceFile.Reader(fs, path, conf);
	}
	
	public boolean next() throws IOException
	{
		Writable key = new Text();
		Writable value = new Text();
		
		boolean result = reader.next(key, value);
		
		currentKey = key.toString();
		currentValue = value.toString();
		
		return result;
	}

	public String getKey() {
		return currentKey;
	}

	public String getValue() {
		return currentValue;
	}
	
	public void close() throws IOException {
		if(reader!=null)
			reader.close();
	}
	
	public static void main(String[] args) throws Exception {
		TextSequenceFileReader r = new TextSequenceFileReader(args[0]);
		
		while(r.next())
		{
			if(r.getKey().equals(args[1]))
			{
				System.out.println(r.getValue());
				break;
			}
		}
		
		r.close();
	}
}
