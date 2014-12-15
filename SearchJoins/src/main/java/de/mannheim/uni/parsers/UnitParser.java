package de.mannheim.uni.parsers;

import de.mannheim.uni.units.SubUnit;
import de.mannheim.uni.units.UnitManager;

/**
 * @author petar
 * 
 */
public class UnitParser {
	UnitManager mgr;

	public UnitParser() {
		mgr = new UnitManager();
	}

	public SubUnit parseUnit(String text) {
		text = text.replaceAll(" ", "");
		try {
			SubUnit sub = mgr.parseUnit(text);
			if (sub != null && !sub.getName().equals("normalized number"))
				return sub;
		} catch (Exception e) {

		}
		return null;
	}
}
