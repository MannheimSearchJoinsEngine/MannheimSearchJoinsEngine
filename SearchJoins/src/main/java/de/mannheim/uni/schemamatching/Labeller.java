package de.mannheim.uni.schemamatching;

import java.util.Map;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.pipelines.Pipeline;

/**
 * Creates pairs of labelled columns according to Infogather: entity augmentation and attribute discovery by holistic matching with web tables Section 4.1.3
 * @author Oliver
 *
 */
public class Labeller {

	public void getLabelledColumns(Pipeline pipe, Map<TableColumn, Map<TableColumn, ColumnScoreValue>> scores)
	{
		for(TableColumn c1 : scores.keySet())
		{
			for(TableColumn c2 : scores.keySet())
			{
				
			}
		}
		
	}
	
}
