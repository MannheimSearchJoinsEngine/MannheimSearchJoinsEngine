package de.mannheim.uni.evaluation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.SearchJoinsObject;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.StringUtils;
import de.mannheim.uni.utils.io.DataStorageStringString;
import de.mannheim.uni.utils.io.DataStorageUtils2;
import de.mannheim.uni.utils.io.DefaultStringStringStorage;

/**
 * Creates and reads dictionaries to be used for evaluation of joined values
 * @author Oliver
 *
 */
public class DictionaryManager extends SearchJoinsObject {

	public DictionaryManager(Pipeline pipeline) {
		super(pipeline);
	}

	public DictionaryManager(SearchJoinsObject obj) {
		super(obj);
	}
	
	private DataStorageUtils2<String, String> getDataStorage()
	{
		DataStorageStringString store = new DefaultStringStringStorage();
		
		return store;
	}
	
	public void buildDictionary(String valuesPath, String dictionaryPath, String header)
	{
		// try to load an existing dictionary
		Map<String, Collection<String>> dictionary = loadDictionary(dictionaryPath);
		
		if(dictionary==null)
			dictionary = new HashMap<String, Collection<String>>();
		
		// load the values
		Collection<Pair<String, String>> values = loadValues(valuesPath, header);
		
		if(values==null)
			return;
		
		// go through table and ask user for every value that is not in the dictionary yet
		fillDictionary(values, dictionary);
		
		// save the new dictionary
		try {
			getDataStorage().saveMapCollection(dictionary, dictionaryPath);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public void buildDictionaryFromTable(Table table, String dictionaryPath, Table originalValues, String script)
	{
		// try to load an existing dictionary
		Map<String, Collection<String>> dictionary = loadDictionary(dictionaryPath);
		
		if(dictionary==null)
			dictionary = new HashMap<String, Collection<String>>();
		
		// go through table and ask user for every value that is not in the dictionary yet
		fillDictionaryFromTable(table, originalValues, dictionary, dictionaryPath, script);
	}
	
	public Map<String, Collection<String>> loadDictionary(String dictionaryPath)
	{	
		try {
			return getDataStorage().loadMapCollection(dictionaryPath);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public Collection<Pair<String, String>> loadValues(String valuesPath, String header)
	{
		try {
			return getDataStorage().loadPairsForHeader(valuesPath, header);
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	private void fillDictionary(Collection<Pair<String, String>> values, Map<String, Collection<String>> dictionary)
	{
		System.out.println("Enter q to quit");
		System.out.println("Enter + to add the value to the dictionary, any other value otherwise");
		
		int i = 0;
		for(Pair<String, String> p : values)
		{
			i++;
			String currentKey = p.getFirst();
			Collection<String> positives = null;
			
			if(dictionary.containsKey(currentKey))
				positives = dictionary.get(currentKey);
			else
			{
				positives = getDataStorage().createCollection2();
				dictionary.put(currentKey, positives);
			}
			
			String currentValue = p.getSecond();
			
			if(!positives.contains(currentValue))
			{
				System.out.println("-----------------------------------" + i + "/" + values.size());
				System.out.println("DBPedia: " + currentKey);
				System.out.println("Existing values: " + StringUtils.join(positives, ", "));
				System.out.println("WEB: " + currentValue);
				
				String input = System.console().readLine();
				
				if(input.equals("+"))
					positives.add(currentValue);
				else
					if(input.equals("q"))
						return;
			}
		}
	}
	
	private void fillDictionaryFromTable(Table t, Table originalValuesTable, Map<String, Collection<String>> dictionary, String dictionaryPath, String script)
	{
		TableColumn key = t.getFirstKey();
		TableColumn ref = null;
		Map<String, String> originalValues = new HashMap<String, String>();
		Map<String, String> originalKeys = new HashMap<String, String>();
		
		for(TableColumn c : t.getColumns())
			if(!c.isKey() && c.getHeader().startsWith("dbpedia_"))
			{
				ref = c;
				break;
			}
		
		System.out.println();
		System.out.println();
		
		if(originalValuesTable!=null)
		{
			System.out.println("Loading reference values from original table");
			
			TableColumn origVal = null;
			for(TableColumn c : originalValuesTable.getColumns())
			{
				if(!c.isKey() && c.getHeader().contains(ref.getHeader().replace("dbpedia_", "")))
					origVal = c;
				break;
			}
			if(origVal == null)
				origVal = originalValuesTable.getColumns().get(1);
			for(Integer i : originalValuesTable.getFirstKey().getValues().keySet())
			{
				String originalKey = originalValuesTable.getFirstKey().getValues().get(i);
				String keyValue = StringNormalizer.clearString(originalKey, true); 
				
				if(origVal.getValues().containsKey(i))
				{
					String newValue = origVal.getValues().get(i);
					
					if(originalValues.containsKey(keyValue))
						newValue = originalValues.get(keyValue) + ", " + newValue;
					
					originalValues.put(keyValue, newValue);
					originalKeys.put(keyValue, originalKey);
				}
			}
			
			System.out.println(originalValues.keySet().size() + " original values for " + t.getFirstKey().getValues().size() + " total values");
		}
		else
			System.out.println("No original table provided!");

		System.out.println("Enter q to quit");
		System.out.println("Enter + to add the value to the dictionary, any other value otherwise");
		
		List<String> skipValues = new ArrayList<String>();
		skipValues.add("string");
		skipValues.add("numeric");
		skipValues.add("date");
		
		int cnt=0;
		for(Integer i : key.getValues().keySet())
		{
			cnt++;
			if(i >= getPipeline().getInstanceStartIndex())
			{
				String currentKey = key.getValues().get(i);
				
				if(originalKeys!=null && originalKeys.containsKey(currentKey))
					currentKey = originalKeys.get(currentKey);
				
				String refValue = ref.getValues().get(i);
				if(originalValues!=null && originalValues.containsKey(currentKey))
					refValue = originalValues.get(currentKey);
				
				System.out.println();
				System.out.println("*********************************************");
				System.out.println("[" + cnt + "/"+ key.getValues().size() + "] " + currentKey + ": " + refValue);

				boolean runScript = true;
				
				Collection<String> values = null;
				
				if(dictionary.containsKey(currentKey))
					values = dictionary.get(currentKey);
				else
				{
					values = getDataStorage().createCollection2();
					dictionary.put(currentKey, values);
				}
				
				for(TableColumn c : t.getColumns())
				{
					String currentValue = c.getValues().get(i);

					Collection<String> wrongValues = getDataStorage().createCollection2();
					
					if(c!=key && c!=ref 
							&& !skipValues.contains(currentValue) 
							&& c.getValues().containsKey(i) 
							&& !PipelineConfig.NULL_VALUE.equalsIgnoreCase(currentValue)
							&& !values.contains(currentValue)
							&& !wrongValues.contains(currentValue))
					{
						String input = null;
						boolean match = false;
						boolean candidate = false;
						
						if(refValue!=null)
						{
							for(String s : refValue.split(","))
							{
								if(currentValue.equals(s.trim()))
								{
									match = true;
									break;
								}
								else if(currentValue.contains(s.trim()) || s.trim().contains(currentValue))
								{
									candidate = true;
								}
								else if(containsAnyTerm(currentValue, s) || containsAnyTerm(s, currentValue))
								{
									candidate = true;
								}
							}
						}
						else
							candidate=true;
						
						if(match || candidate)
						{
							System.out.print(currentValue);
							if(runScript)
							{
								// run the script only once per key and only if a user decision is needed
								runScript(script, currentKey, currentValue);
								runScript = false;
							}
						}
						else
							continue;
						
						if(match)
							System.out.println("+");
						else if(candidate)
							input = System.console().readLine();
						
						if(match || input.equals("+") )
						{
							values.add(currentValue);
							
							// save the new dictionary
							try {
								getDataStorage().saveMapCollection(dictionary, dictionaryPath);
							} catch (IOException e) {
								e.printStackTrace();
							}
						}
						else
							if(input.equals("q"))
								return;
							else
								wrongValues.add(currentValue);
					}
				}
			}
		}
	}
	
	private void runScript(String script, String key, String value)
	{
		if(script==null)
			return;
		
		try {
			for(String s : script.split("\\$\\$"))
			{
				s = script.replace("$key", key.replace(" ", "%20"));
				s = s.replace("$value", value.replace(" ", "%20"));
				
				//System.out.println(s);
				Process p = Runtime.getRuntime().exec(s);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private boolean containsAnyTerm(String s1, String s2)
	{
		String[] terms = s1.split(" ");
		
		List<String> lst = new LinkedList<String>();
		for(String s : s2.split(" "))
			lst.add(s);
		
		for(String s : terms)
			if(lst.contains(s))
			{
				return true;
			}
		
		return false;
	}
}
