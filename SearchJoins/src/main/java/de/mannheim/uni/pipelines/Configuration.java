package de.mannheim.uni.pipelines;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Enumeration;
//import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeSet;

public abstract class Configuration {
	//TODO have a look at http://commons.apache.org/proper/commons-configuration/
	
	private Properties properties;
	
	private HashMap<String, String> defaults;
	
	public Configuration()
	{
		defaults = new HashMap<String, String>();
		properties = new Properties() {
		    @Override
		    public synchronized Enumeration<Object> keys() {
		        return Collections.enumeration(new TreeSet<Object>(super.keySet()));
		    }
		};
		
		initialise();
		
		setDefaults();
	}
	
	public void setDefaults()
	{
		for(Entry<String, String> e : defaults.entrySet())
			properties.setProperty(e.getKey(), e.getValue());
	}
	
	public boolean loadFromFile(String configFile)
	{
		try {
			properties.load(new FileInputStream(new File(configFile)));
			return true;
		} catch (Exception e) {
			System.out.println("Error while trying to read " + configFile);
			e.printStackTrace();
			return false;
		}
	}
	
	public void save(String configFile)
	{
		try {
			OutputStream stream = new FileOutputStream(new File(configFile));
			properties.store(stream, "");
			stream.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	protected abstract void initialise();
	
	protected void addProperty(String key, String defaultValue)
	{
		defaults.put(key, defaultValue);
	}
	
	public String getPropertyValue(String key)
	{
		if(!defaults.containsKey(key))
		{
			System.out.println("No default value for property " + key + " was set!");
			return properties.getProperty(key);
		}
		else
			return properties.getProperty(key, defaults.get(key));
	}
	
	public void setPropertyValue(String key, String value)
	{
		if(!defaults.containsKey(key))
		{
			System.out.println("No default value for property " + key + " was set, using " + value + "!");
			defaults.put(key, value);
		}
		
		properties.setProperty(key, value);
	}
	
	public void setValue(String key, Object value)
	{
		setPropertyValue(key, value.toString());
	}
	
	public boolean getBool(String key)
	{
		return Boolean.valueOf(getPropertyValue(key));
	}
	
	public int getInt(String key)
	{
		return Integer.valueOf(getPropertyValue(key));
	}
	
	public long getLong(String key)
	{
		return Long.valueOf(getPropertyValue(key));
	}
	
	public float getFloat(String key)
	{
		return Float.valueOf(getPropertyValue(key));
	}
	
	public double getDouble(String key)
	{
		return Double.valueOf(getPropertyValue(key));
	}
	
	/*
	private void setMemberValue(String memberName, Object value)
	{
		try {
			Class c = this.getClass();           

	        Field allFieldArray[] = c.getDeclaredFields();                       
	        int size = allFieldArray.length;
	        for(int i = 0 ; i < size ; i++)
	        {
	            Field f = allFieldArray[i];
	            
	            if(f.getName().equals(memberName))
	            {
					f.set(this, value);	
	            	break;
	            }
	        }
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}*/
}
