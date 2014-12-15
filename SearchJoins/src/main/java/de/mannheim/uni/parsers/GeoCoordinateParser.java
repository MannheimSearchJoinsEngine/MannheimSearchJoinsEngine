package de.mannheim.uni.parsers;

public class GeoCoordinateParser {

	/**
	 * this regex is more complex and catches all types of coordinates, but
	 * often catches single double numbers
	 */
	public static final String GeoCoordRegex = "([SNsn][\\s]*)?((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ms'′\"″,\\.\\dNEWnew]?)|(?:[^ms'′\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ds°\"″,\\.\\dNEWnew]?)|(?:[^ds°\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))[^dm°'′,\\.\\dNEWnew]*))))([SNsn]?)[^\\dSNsnEWew]+([EWew][\\s]*)?((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ms'′\"″,\\.\\dNEWnew]?)|(?:[^ms'′\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ds°\"″,\\.\\dNEWnew]?)|(?:[^ds°\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))[^dm°'′,\\.\\dNEWnew]*))))([EWew]?)";

	public static final String GEO_COORD_REGEX_SIMPLE = "^([-+]?\\d{1,2}([.]\\d+)?),?\\s+([-+]?\\d{1,3}([.]\\d+)?)$";

	public static boolean parseGeoCoordinate(String text) {
		if (text.toLowerCase().matches(GEO_COORD_REGEX_SIMPLE)) {
			return true;
		}
		return false;
	}

	public static void main(String[] args) {
		System.out.println(parseGeoCoordinate("41.1775 20.6788"));
	}
}
