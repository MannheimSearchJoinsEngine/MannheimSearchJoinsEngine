package de.mannheim.uni.infogather.preprocessing;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

public class HtmlContextExtractor {

	public static String extractContext(String html, int tableStart, int tableEnd)
	{
		if(html.length()==0)
			return "";
		
		int start = tableStart;
		int end = tableEnd;
		
		// remove table
		start = Math.max(1, Math.min(html.length(),start)); // never smaller than 1 and never larger than string length
		end = Math.min(html.length()-1, Math.max(start, end)); // never larger than string length and at least start
		
		//html = html.substring(0, start-1) + "&nbsp;" + html.substring(end);
		
		// replace table with some tag that we can select ....
		html = html.substring(0, start-1) + "&nbsp;<mytag class=\"quiteuniqueclassnameforselection\" />&nbsp;" + html.substring(end);
		// and take the text of all siblings
		//String text = doc.select("mytag.quiteuniqueclassnameforselection").get(0).parent().text();
		
		String text = "";
		
		// parse html
		Document doc = Jsoup.parse(html);

		if(doc!=null && doc.body()!=null)
		{
			Elements q = doc.select("mytag.quiteuniqueclassnameforselection");
			
			if(q!=null && q.size()>0)
			{
				Element table = doc.select("mytag.quiteuniqueclassnameforselection").get(0);
				
				if(table!=null)
				{
					Element parent = table;
					
					// loop through the parent elements until one with text is found
					do
					{
						parent = parent.parent();
						
						if(parent!=null)
							text = parent.text().trim(); 
						
					} while(text.isEmpty() && parent!=null);
					
					// make sure the text is not too long
					text = text.substring(0, Math.min(text.length(), 1000) - 1);
					
					/*
					if(table.firstElementSibling()!=null)
						// use the text of the first sibling element
						text = table.firstElementSibling().text().trim();
					
					// if this is empty, use the text of the parent element
					if(text.isEmpty())
					{
						if(table.parent()!=null)
							text = table.parent().text().trim();
					}
					
					// if this is also empty, use the text of the parent's first sibling element
					if(text.isEmpty())
					{
						if(table.parent()!=null && table.parent().firstElementSibling()!=null)
							text = table.parent().firstElementSibling().text().trim();
					}*/
							
					// extract text content
					 //text = doc.body().text();
				}
			}
		}
		
		return text;
	}
	
}
