package de.mannheim.uni.utils;

import java.util.Collection;

public class StringUtils {

	public static String join(Collection<?> values, String delimiter)
	{
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(Object value : values)
		{
			if(!first)
				sb.append(delimiter);
			
			sb.append(value.toString());
			
			first = false;
		}
		
		return sb.toString();
	}
	
}
