package de.mannheim.uni.parsers;

public class NumericParser {

	public static boolean parseNumeric(String text) {

		try {
			Double.parseDouble(text);

			return true;
		} catch (NumberFormatException e) {
		}

		// go char by char and see if it is code or some other number
		// this should be changed with units
		int nmNumbers = 0;
		int nmChars = 0;

		for (char ch : text.toCharArray()) {
			if (Character.isDigit(ch))
				nmNumbers++;
			else if (!Character.isSpace(ch))
				nmChars++;
		}
		if (nmNumbers >= 1.5 * nmChars)
			return true;
		return false;
	}

}
