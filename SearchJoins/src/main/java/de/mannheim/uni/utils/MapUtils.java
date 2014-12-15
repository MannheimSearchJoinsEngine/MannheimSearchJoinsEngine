package de.mannheim.uni.utils;

import java.util.Map;
import java.util.Map.Entry;

import de.mannheim.uni.model.TableColumn.ColumnDataType;

/**
 * @author petar
 *
 */
public class MapUtils {

	public static Object getMaxElementMap(Map<Object, Object> map) {
		double maxCount = (Double) map.values().iterator().next();
		Object type = map.keySet().iterator().next();
		for (Entry<Object, Object> entry : map.entrySet()) {
			if ((Double) entry.getValue() > maxCount) {
				maxCount = (Double) entry.getValue();
				type = entry.getKey();
			}
		}
		return type;
	}
}
