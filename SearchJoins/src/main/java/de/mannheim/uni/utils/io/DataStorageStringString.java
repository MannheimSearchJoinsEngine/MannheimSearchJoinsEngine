package de.mannheim.uni.utils.io;

import java.util.Collection;
import java.util.Map;

public abstract class DataStorageStringString extends DataStorageUtils2<String, String> {

	@Override
	public String createValueFromString1(String value) {
		return value;
	}

	@Override
	public String createValueFromString2(String value) {
		return value;
	}

}
