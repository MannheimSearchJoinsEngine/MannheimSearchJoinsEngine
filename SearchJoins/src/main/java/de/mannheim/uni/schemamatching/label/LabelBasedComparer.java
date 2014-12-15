package de.mannheim.uni.schemamatching.label;

import java.util.ArrayList;
import java.util.List;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.Levenstein;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.lod.SPARQLEndpointQueryRunner;
import de.mannheim.uni.lod.WordnetAPI;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.NGram;

/**
 * @author petar
 * 
 */
public class LabelBasedComparer {

	public static final double DISTANCE_THRESHOLD = 0;
	public static final String DBPEDIA_URI_PREFIX = "http://dbpedia.org/ontology/";
	public static final String YAGO_URI_PREFIX = "http://yago-knowledge.org/resource/";

	// hard coded scores
	public static final double DBPEDIA_EXACT_MATCH_SCORE = 5;
	public static final double YAGO_EXACT_MATCH_SCORE = 5;
	public static final double WORDNET_EXACT_MATCH_SCORE = 5;
	public static final double WORDNET_SYNSET_MATCH_SCORE = 2;

	public static final double DBPEDIA_EXACT_MATCH_SCORE_DUPLICATES = 2;
	public static final double YAGO_EXACT_MATCH_SCORE_DUPLICATES = 2;
	public static final double WORDNET_EXACT_MATCH_SCORE_DUPLICATES = 2;
	public static final double WORDNET_SYNSET_MATCH_SCORE_DUPLICATES = 1.5;

	public static List<String> namesToAvoid;

	public Table queryTable;

	public List<String> queryTableColumnHeaders;

	public IndexManager indexManager;

	public double dbpediaAndYagoExactMatch;
	public double wordnetExactMatch;
	public double wordnetSynsetMatch;

	public LabelBasedComparer(Pipeline pipeline, Table queryTable,
			boolean isTableNeeded) {
		this.wordnetAPI = WordnetAPI.getInstance(pipeline
				.getWordnetDictionaryPath());
		if (isTableNeeded) {
			populateNamesToAvoid();
			this.queryTable = queryTable;
			populateQueryTableColumnHeaders();
			this.indexManager = pipeline.getIndexManager();
		}
		dbpediaAndYagoExactMatch = YAGO_EXACT_MATCH_SCORE;
		wordnetExactMatch = WORDNET_EXACT_MATCH_SCORE;
		wordnetSynsetMatch = WORDNET_SYNSET_MATCH_SCORE;
	}

	public static void populateNamesToAvoid() {
		namesToAvoid = new ArrayList<String>();
		namesToAvoid.add("table csv gz");
		namesToAvoid.add("table gz");
		namesToAvoid.add("table csv");
	}

	private WordnetAPI wordnetAPI;

	public void populateQueryTableColumnHeaders() {
		queryTableColumnHeaders = new ArrayList<String>();
		for (TableColumn col : queryTable.getColumns()) {
			queryTableColumnHeaders.add(col.getHeader());
		}
	}

	public static String cleanFileName(String fileName) {
		return fileName.replace(".csv.gz", "").replace(".gz", "")
				.replace(".csv", "");
	}

	public void setDbpediaAndYagoExactMatch(double dbpediaAndYagoExactMatch) {
		this.dbpediaAndYagoExactMatch = dbpediaAndYagoExactMatch;
	}

	public void setWordnetExactMatch(double wordnetExactMatch) {
		this.wordnetExactMatch = wordnetExactMatch;
	}

	public void setWordnetSynsetMatch(double wordnetSynsetMatch) {
		this.wordnetSynsetMatch = wordnetSynsetMatch;
	}

	/**
	 * matches strings based on wordnet synsets
	 * 
	 * @param leftString
	 * @param rightString
	 * @return
	 */
	public double matchStrings(String leftString, String rightString) {
		double priceForMatching = 1;
		if (leftString.length() < 3 || rightString.length() < 3)
			return 0;
		boolean stringsMatch = false;
		try {
			leftString = StringNormalizer.clearString(leftString, false);
			rightString = StringNormalizer.clearString(rightString, false);
			if (leftString.length() < 3 || rightString.length() < 3)
				return 0;

			// try to match synonyms
			// System.out.println("Searching Synonyms" + leftString + ":"
			// + rightString);
			List<String> leftList = wordnetAPI.getSynonyms(leftString);
			List<String> rightList = wordnetAPI.getSynonyms(rightString);

			// if the strings were recognized and are the same, return full
			// score
			if (leftList != null && rightList != null
					&& leftString.equals(rightString))
				priceForMatching = wordnetExactMatch;
			// see if the strings are equal
			if (leftString.equals(rightString))
				priceForMatching = (wordnetSynsetMatch + 0.2);

			if (matchTwoLists(leftList, rightList)) {
				stringsMatch = true;
				priceForMatching = wordnetSynsetMatch;

			}

			// try to match synonyms, hyponyms and hypernyms
			// System.out.println("Searching Hypernyms" + leftString + ":"
			// + rightString);
			leftList.addAll(wordnetAPI.getHypernyms(leftString));
			rightList.addAll(wordnetAPI.getHypernyms(rightString));
			leftList.addAll(wordnetAPI.getHyponyms(leftString));
			rightList.addAll(wordnetAPI.getHyponyms(rightString));
			if (matchTwoLists(leftList, rightList)) {
				stringsMatch = true;
				priceForMatching = wordnetSynsetMatch;
			}
			// if still are not the same try with n-grams of size 3
			// System.out.println("populating ngrams" + leftString + ":"
			// + rightString);
			List<String> leftNgrams = NGram.getAllNgramsInBound(leftString, 1,
					2);
			List<String> rightNgrams = NGram.getAllNgramsInBound(rightString,
					1, 2);
			if (leftList.size() == 0)
				leftList = populateNgramList(leftNgrams, leftList);
			if (rightList.size() == 0)
				rightList = populateNgramList(rightNgrams, rightList);

			if (matchTwoLists(leftList, rightList)) {
				priceForMatching = wordnetSynsetMatch;
			}
		} catch (Exception e) {

		}
		// Levenstein lev = new Levenstein();
		// double score = lev.score(leftString, rightString);
		//
		// if (leftString.contains(rightString)
		// || rightString.contains(leftString)
		// || score >= DISTANCE_THRESHOLD) {
		// stringsMatch = true;
		// return wordnetExactMatch;
		// }
		// if (leftString.equals(rightString))
		// return wordnetExactMatch;
		// System.out.println("final mathching" + leftString + ":" +
		// rightString);
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);
		return sim.score(leftString, rightString) * priceForMatching;
	}

	/**
	 * checks if the leftString is class in YAGO and if the rightString is in
	 * the superclasses of the retrieved class
	 * 
	 * @param leftString
	 * @param rightString
	 * @return 0 if the lefString or rightString is not a DBpedia class; -1 if
	 *         it is a dbpedia class but the rightString is not in its
	 *         superClasses; 1 if the rigtString is superClass of the leftString
	 */
	public int checkYAGOClasses(String leftString, String rightString,
			SPARQLEndpointQueryRunner queryRunner) {
		try {
			if (leftString.equals(rightString))
				return 1;
			queryRunner = new SPARQLEndpointQueryRunner(
					SPARQLEndpointQueryRunner.YAGO_ENDPOINT);
			queryRunner.setPageSize(10000);

			String leftClassName = queryRunner.getClassFromLabel(leftString);

			if (leftClassName == null)
				return 0;
			if (leftClassName.equals(YAGO_URI_PREFIX + rightString))
				return 1;
			List<String> rightSuperClasses = queryRunner
					.getSuperClasses(YAGO_URI_PREFIX + rightString);

			if (rightSuperClasses.size() == 0) {
				String rightStringClass = queryRunner
						.getClassFromLabel(rightString);
				if (rightStringClass == null)
					return 0;
				else
					rightSuperClasses = queryRunner
							.getSuperClasses(rightStringClass);
			}
			List<String> superClasses = queryRunner
					.getSuperClasses(leftClassName);

			if (superClasses.size() == 0)
				return 0;
			for (String clazz : superClasses) {
				if (clazz.equals(YAGO_URI_PREFIX + rightString))
					return 1;
			}
			if (compareTwoLists(superClasses, rightSuperClasses))
				return 1;
		} catch (Exception e) {

		}
		return -1;
	}

	/**
	 * checks if the leftString is class in DBpedia and if the rightString is in
	 * the superclasses of the retrieved class
	 * 
	 * @param leftString
	 * @param rightString
	 * @return 0 if the lefString or rightString is not a DBpedia class; -1 if
	 *         it is a dbpedia class but the rightString is not in its
	 *         superClasses; 1 if the rigtString is superClass of the leftString
	 */
	public int checkDBpediaClasses(String leftString, String rightString,
			SPARQLEndpointQueryRunner queryRunner) {
		try {
			leftString = leftString.replaceFirst(Character.toString(leftString
					.charAt(0)), Character.toString(Character
					.toUpperCase(leftString.charAt(0))));
			rightString = rightString.replace(rightString.charAt(0),
					Character.toUpperCase(rightString.charAt(0)));
			if (leftString.equals(rightString))
				return 1;
			queryRunner = new SPARQLEndpointQueryRunner(
					SPARQLEndpointQueryRunner.DBPEDIA_ENDPOINT);
			queryRunner.setPageSize(10000);
			List<String> rightSuperClasses = queryRunner
					.getSuperClasses(DBPEDIA_URI_PREFIX + rightString);

			if (rightSuperClasses.size() == 0)
				return 0;
			List<String> superClasses = queryRunner
					.getSuperClasses(DBPEDIA_URI_PREFIX + leftString);
			if (superClasses.size() == 0)
				return 0;
			for (String clazz : superClasses) {
				if (clazz.equals(DBPEDIA_URI_PREFIX + rightString))
					return 1;
			}
			if (compareTwoLists(superClasses, rightSuperClasses))
				return 1;
		} catch (Exception e) {

		}
		return -1;
	}

	public static boolean compareTwoLists(List<String> leftList,
			List<String> rightList) {
		if (leftList.size() != rightList.size())
			return false;
		for (String str : leftList) {
			if (!rightList.contains(str))
				return false;
		}
		return true;
	}

	private List<String> populateNgramList(List<String> ngrams,
			List<String> synsets) {
		synsets = new ArrayList<String>();
		int previousNgramSize = 0;
		for (String ngram : ngrams) {
			int currentSize = countWhitspaces(ngram);
			List<String> synonyms = wordnetAPI.getSynonyms(ngram);
			if (synonyms.size() == 0)
				continue;
			if (synonyms.size() != 0 && currentSize > previousNgramSize)
				synsets.clear();
			synsets.addAll(synonyms);
			synsets.addAll(wordnetAPI.getHypernyms(ngram));
			// synsets.addAll(wordnetAPI.getHyponyms(ngram));
			previousNgramSize = currentSize;
		}
		return synsets;

	}

	private int countWhitspaces(String value) {
		int size = 0;
		for (char ch : value.toCharArray())
			if (Character.isWhitespace(ch))
				size++;

		return size;
	}

	public boolean matchTwoLists(List<String> leftList, List<String> rightList) {
		Levenstein lev = new Levenstein();
		for (String leftStr : leftList) {
			for (String rightStr : rightList) {
				if (lev.score(leftStr, rightStr) >= DISTANCE_THRESHOLD)
					return true;
			}
		}
		return false;

	}

	public List<String> populateEntityTableHeaders(String tableName) {
		List<String> entityTableColumnHeaders = new ArrayList<String>();
		List<IndexEntry> entries = indexManager.getRowValues(tableName, 5);
		for (IndexEntry en : entries) {
			entityTableColumnHeaders.add(en.getColumnHeader());
		}
		return entityTableColumnHeaders;
	}

	public static void main(String[] args) {
		// LabelBasedComparer comp = new LabelBasedComparer(
		// Pipeline.getPipelineFromConfigFile("", "searchJoins.conf"),
		// null);
		// System.out.println(comp.matchStrings("vat", "dec mean c"));
		// System.out.println(checkDBpediaClasses("river", "BodyOfWater",
		// new SPARQLEndpointQueryRunner("")));
	}
}
