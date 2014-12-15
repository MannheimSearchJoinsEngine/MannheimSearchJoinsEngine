package de.mannheim.uni.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SequenceFileWriter<TKey extends Writable, TValue extends Writable> {
	private String fileName;

	private Writer writer;
	
	public SequenceFileWriter(String file, Class<?> keyClass, Class<?> valueClass) throws Exception {
		fileName = file;
		
		Path path = new Path(fileName);
		LocalSetup local = new LocalSetup();
		
		Configuration conf = local.getConf();
		FileSystem fs = local.getLocalFileSystem();
		
		writer = SequenceFile.createWriter(fs, conf, path, keyClass, valueClass, CompressionType.BLOCK);
	}
	
	public void write(TKey key, TValue value) throws IOException
	{
		writer.append(key, value);
	}

	public void close() throws IOException {
		if(writer!=null)
			writer.close();
	}
}