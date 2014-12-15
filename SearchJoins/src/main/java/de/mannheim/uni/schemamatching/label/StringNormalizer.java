package de.mannheim.uni.schemamatching.label;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.core.LetterTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.snowball.SnowballFilter;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.CharArraySet;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

import scala.Math;

import com.sun.jdi.Value;
import com.wcohen.ss.Jaccard;

import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class StringNormalizer {

	public static String clearString(String value, boolean removeBrackets) {
		return clearString(value, removeBrackets, false);
	}

	public static String clearString(String value, boolean removeBrackets,
			boolean useStemmer) {
		try {

			String cleanStr = "";
			for (String str : tokenizeString(value, removeBrackets, useStemmer))
				cleanStr += " " + str;
			cleanStr = cleanStr.replaceFirst(" ", "");
			if (cleanStr.equals(""))
				cleanStr = PipelineConfig.NULL_VALUE;
			return cleanStr;
		} catch (Exception e) {
			return value;
		}

	}

	public static String clearString4FastJoin(String value,
			boolean removeBrackets, boolean useStemmer) {
		String v = clearString(value, removeBrackets, useStemmer);

		v = StringNormalizer.clearString(v, false, useStemmer);
		v = v.replaceAll("\\P{InBasic_Latin}", "");
		int minLength = 127;
		if (v.length() < minLength)
			minLength = v.length();
		v = v.substring(0, minLength);

		return v;
	}

	public static List<String> tokenizeString(String string,
			boolean removeBrackets) {
		return tokenizeString(string, removeBrackets, false);
	}

	public static List<String> tokenizeString(String string,
			boolean removeBrackets, boolean useStemmer) {

		string = string.replace("&nbsp;", " ");
		string = string.replace("&nbsp", " ");
		string = string.replace("nbsp", " ");

		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		List<String> result = new ArrayList<String>();

		try {

			Map<String, String> args = new HashMap<String, String>();
			args.put("generateWordParts", "1");
			args.put("generateNumberParts", "1");
			args.put("catenateNumbers", "0");
			args.put("splitOnCaseChange", "1");
			WordDelimiterFilterFactory fact = new WordDelimiterFilterFactory(
					args);
			// fact.init(args);

			// TokenStream stream = analyzer.tokenStream(null, new StringReader(
			// string));
			// resolve non unicode chars
			string = org.apache.commons.lang.StringEscapeUtils
					.unescapeJava(string);
			// remove content in brackets
			if (removeBrackets) {
				string = string.replaceAll("\\(.*\\)", "");
			}
			TokenStream stream = fact.create(new WhitespaceTokenizer(
					Version.LUCENE_46, new StringReader(string)));
			stream.reset();

			if (useStemmer)
				stream = new PorterStemFilter(stream);
			stream = new LowerCaseFilter(Version.LUCENE_46, stream);
			stream = new StopFilter(Version.LUCENE_46, stream,
					((StopwordAnalyzerBase) analyzer).getStopwordSet());

			if (Pipeline.getCurrent() != null
					&& Pipeline.getCurrent().getCustomStopWordsFile() != null) {
				CharArraySet cas = new CharArraySet(Version.LUCENE_46, Pipeline
						.getCurrent().getCustomStopWords(), true);
				stream = new StopFilter(Version.LUCENE_46, stream, cas);
			}

			while (stream.incrementToken()) {
				result.add(stream.getAttribute(CharTermAttribute.class)
						.toString());
			}
			stream.close();
		} catch (IOException e) {
			// not thrown b/c we're using a string reader...
			result = new ArrayList();
			result.add(string);
		}
		if (string.contains("$")) {
			if (result.size() > 0 && !result.get(0).equals(string))
				result.add("$");
		}
		return result;
	}

	public static String removeCustomStopwords(String s) {
		List<String> stopwords = Pipeline.getCurrent().getCustomStopWords();

		if (stopwords == null || stopwords.size() == 0)
			return s;

		String result = s;

		for (String stop : stopwords) {
			result = result.replace(stop, "");
		}

		result = result.replace("  ", " ");

		return result;
	}

	public static void main(String[] args) throws IOException {
		System.out.println(clearString("founded by founder editing editor",
				true));
		// List<String> strs =
		// tokenizeString("RoadCar.csv is working on the construction");
		// for (String str : strs) {
		// System.out.println(str);
		// }

		// Jaccard lev = new Jaccard();
		// System.out.println(lev.score("STREAM", "BODY OF WATER streaam"));
	}
}
