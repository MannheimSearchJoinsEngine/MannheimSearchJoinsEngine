package de.mannheim.uni.parsers;

public class BooleanParser {

	public static final String booleanRegex = "(yes|true|1|no|false|0)";

	public static boolean parseBoolean(String text) {

		if (text.toLowerCase().matches(booleanRegex)) {
			return true;
		}
		return false;
	}
	public static void main(String[] args) {
		System.out.println(parseBoolean("dfsd"));
	}
}
