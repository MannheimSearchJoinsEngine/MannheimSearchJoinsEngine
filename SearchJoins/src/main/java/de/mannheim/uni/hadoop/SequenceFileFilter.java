package de.mannheim.uni.hadoop;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;

public class SequenceFileFilter {

	TextSequenceFileReader _reader;
	TextSequenceFileWriter _writer;
	
	public SequenceFileFilter(String input, String output) throws Exception
	{
		_reader = new TextSequenceFileReader(input);
		_writer = new TextSequenceFileWriter(output);
	}
	
	public void filter(List<String> allowedKeys) throws IOException
	{
		int cnt=0;
		while(_reader.next())
		{
			System.out.println(_reader.getKey());
			if(allowedKeys.contains(_reader.getKey()))
			{
				_writer.write(_reader.getKey(), _reader.getValue());
				cnt++;
			}
		}
		System.out.println(cnt + " entries written.");
	}
	
	public void filter(int firstN) throws IOException
	{
		int cnt=0;
		while(_reader.next())
		{
			if(cnt++<firstN)
			{
				_writer.write(_reader.getKey(), _reader.getValue());
			}
			else
				break;
		}
		System.out.println(cnt + " entries written.");
	}
	
	public static void main(String[] args) throws Exception {
		List<String> keys = FileUtils.readLines(new File(args[2]));
		
		SequenceFileFilter sff = new SequenceFileFilter(args[0], args[1]);
		
		sff.filter(keys);
		//sff.filter(Integer.parseInt(args[2]));
	}
}
