package de.mannheim.uni.hadoop;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class SequenceFileReader<TKey extends Writable, TValue extends Writable> {
	private String fileName;
	private TKey currentKey = null;
	private TValue currentValue = null;
	private SequenceFile.Reader reader;
	private Class<?> keyClass;
	private Class<?> valueClass;
	
	public SequenceFileReader(String inputFile, Class<?> keyClass, Class<?> valueClass) throws Exception {
		fileName = inputFile;
		
		this.keyClass = keyClass;
		this.valueClass = valueClass;
		
		File f = new File(fileName);
		if(!f.exists())
			throw new Exception("File does not exist!");
		
		Path path = new Path(fileName);
		LocalSetup local = new LocalSetup();
		
		Configuration conf = local.getConf();
		FileSystem fs = local.getLocalFileSystem();
		
		reader = new SequenceFile.Reader(fs, path, conf);
	}
	
	@SuppressWarnings("unchecked")
	public boolean next() throws IOException, InstantiationException, IllegalAccessException
	{
		TKey key = (TKey) keyClass.newInstance();
		TValue value = (TValue) valueClass.newInstance();
		
		boolean result = reader.next(key, value);
		
		currentKey = key;
		currentValue = value;
		
		return result;
	}

	public TKey getKey() {
		return currentKey;
	}

	public TValue getValue() {
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
