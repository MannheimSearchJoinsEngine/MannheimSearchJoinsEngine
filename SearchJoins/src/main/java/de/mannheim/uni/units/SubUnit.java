package de.mannheim.uni.units;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;

/**
 * @author petar
 * 
 */
public class SubUnit
	implements java.io.Serializable
{

	private String name;

	boolean isConvertible;

	private double rateToConvert;

	private List<String> abbrevations;
	private HashMap<String, Pattern> abbrPatterns1;
	private HashMap<String, Pattern> abbrPatterns2;
	
	private Unit baseUnit;

	private String newValue;

	public String getNewValue() {
		return newValue;
	}

	public void setNewValue(String newValue) {
		this.newValue = newValue;
	}

	public Unit getBaseUnit() {
		return baseUnit;
	}

	public void setBaseUnit(Unit baseUnit) {
		this.baseUnit = baseUnit;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public boolean isConvertible() {
		return isConvertible;
	}

	public void setConvertible(boolean isConvertible) {
		this.isConvertible = isConvertible;
	}

	public double getRateToConvert() {
		return rateToConvert;
	}

	public void setRateToConvert(double rateToConvert) {
		this.rateToConvert = rateToConvert;
	}

	public List<String> getAbbrevations() {
		return abbrevations;
	}

	public void setAbbrevations(List<String> abbrevations) {
		this.abbrevations = abbrevations;
		
		for(String s : abbrevations)
		{
			abbrPatterns1.put(s, Pattern.compile("\\d{1,20}.*" + Pattern.quote(s.toLowerCase())));
			abbrPatterns2.put(s, Pattern.compile(Pattern.quote(s.toLowerCase()) + ".*\\d{1,20}"));
			
		}		
		
	}

	public void setAbbrevationsFromStringField(String[] abbrs) {

		for (String str : abbrs) {
			abbrevations.add(str.replace("\"", ""));
		}
	}

	public SubUnit() {
		abbrevations = new ArrayList<String>();
		abbrPatterns1 = new HashMap<String, Pattern>();
		abbrPatterns2 = new HashMap<String, Pattern>();
	}
	
	public String getMatchingAbbreviation(String text)
	{
		for (String unitName : getAbbrevations()) {
			if(abbrPatterns1.get(unitName).matcher(text).matches())
				return unitName;
		}
		return null;
	}
	
	public String getMatchingAbbreviation2(String text)
	{
		for (String unitName : getAbbrevations()) {
			if(abbrPatterns2.get(unitName).matcher(text).matches())
				return unitName;
		}
		return null;
	}
}
