package de.mannheim.uni.statistics;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.pipelines.Pipeline;

public class DataLogger {

	private boolean isEnabled;
	
	public DataLogger(Pipeline pipe)
	{
		isEnabled=true;
	}
	
	public void logTable(Table table, String name)
	{
		if(isEnabled)
			table.writeTableToFile(name);
	}

	public void logMap(Map<?,?> map, String name)
	{
		if(isEnabled)
		{
			try {
				BufferedWriter w = new BufferedWriter(new FileWriter(name + ".csv"));
				
				w.write("key\tvalue\n");
				
				for(Entry<?, ?> e : map.entrySet())
					w.write(e.getKey().toString() + "\t" + e.getValue().toString() + "\n");
				
				w.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void logMapMap(Map<?,?> map, String name)
	{
		if(isEnabled)
		{
			try {
				BufferedWriter w = new BufferedWriter(new FileWriter(name + ".csv"));
				
				w.write("key1\tkey2\tvalue\n");
				
				for(Entry<?, ?> e0 : map.entrySet())
					for(Entry<?,?> e : ((Map<?,?>)e0.getValue()).entrySet())
						w.write(e0.getKey().toString() + "\t" + e.getKey().toString() + "\t" + e.getValue().toString() + "\n");
				
				w.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
