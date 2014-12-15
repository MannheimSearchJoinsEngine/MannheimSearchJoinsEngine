package de.mannheim.uni.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class TextSequenceFileWriter {
	private String fileName;

	private Writer writer;
	
	public TextSequenceFileWriter(String file) throws Exception {
		fileName = file;
		
		Path path = new Path(fileName);
		LocalSetup local = new LocalSetup();
		
		Configuration conf = local.getConf();
		FileSystem fs = local.getLocalFileSystem();
		
		writer = SequenceFile.createWriter(fs, conf, path, Text.class, Text.class);
	}
	
	public void write(String key, String value) throws IOException
	{
		writer.append(new Text(key), new Text(value));
	}

	public long getApproximateLength() throws IOException
	{
		return writer.getLength();
	}
	
	public void close() throws IOException {
		if(writer!=null)
			writer.close();
	}
}
