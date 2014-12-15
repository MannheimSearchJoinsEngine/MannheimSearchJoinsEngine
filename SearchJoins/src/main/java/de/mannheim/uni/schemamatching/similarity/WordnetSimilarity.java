package de.mannheim.uni.schemamatching.similarity;

import java.util.ArrayList;
import java.util.List;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.Levenstein;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.mannheim.uni.lod.WordnetAPI;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.LabelBasedComparer;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.NGram;

public class WordnetSimilarity {

	Pipeline p;
	WordnetAPI wordnet;
	
	double wordnetExactMatch = LabelBasedComparer.WORDNET_EXACT_MATCH_SCORE_DUPLICATES;
	//double dbpediaAndYagoExactMatch = LabelBasedComparer.WORDNET_EXACT_MATCH_SCORE_DUPLICATES;
	double wordnetSynsetMatch = LabelBasedComparer.WORDNET_SYNSET_MATCH_SCORE_DUPLICATES;
	double DISTANCE_THRESHOLD = 0;
	
	public void setWordnetExactMatch(double wordnetExactMatch) {
		this.wordnetExactMatch = wordnetExactMatch;
	}
	
	public void setWordnetSynsetMatch(double wordnetSynsetMatch) {
		this.wordnetSynsetMatch = wordnetSynsetMatch;
	}
	
	public void setDISTANCE_THRESHOLD(double dISTANCE_THRESHOLD) {
		DISTANCE_THRESHOLD = dISTANCE_THRESHOLD;
	}
	
	public WordnetSimilarity(Pipeline pipeline)
	{
		p = pipeline;
		
		wordnet = WordnetAPI.getInstance(pipeline
				.getWordnetDictionaryPath());
	}
	
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
			List<String> leftList = wordnet.getSynonyms(leftString);
			List<String> rightList = wordnet.getSynonyms(rightString);

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
			leftList.addAll(wordnet.getHypernyms(leftString));
			rightList.addAll(wordnet.getHypernyms(rightString));
			leftList.addAll(wordnet.getHyponyms(leftString));
			rightList.addAll(wordnet.getHyponyms(rightString));
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
		
		NGramTokenizer tok = new NGramTokenizer(2, 4, true,
				new SimpleTokenizer(true, true));
		Jaccard sim = new Jaccard(tok);
		return sim.score(leftString, rightString) * priceForMatching;
	}
	
	private boolean matchTwoLists(List<String> leftList, List<String> rightList) {
		Levenstein lev = new Levenstein();
		for (String leftStr : leftList) {
			for (String rightStr : rightList) {
				if (lev.score(leftStr, rightStr) >= DISTANCE_THRESHOLD)
					return true;
			}
		}
		return false;

	}
	
	private int countWhitspaces(String value) {
		int size = 0;
		for (char ch : value.toCharArray())
			if (Character.isWhitespace(ch))
				size++;

		return size;
	}
	
	private List<String> populateNgramList(List<String> ngrams,
			List<String> synsets) {
		synsets = new ArrayList<String>();
		int previousNgramSize = 0;
		for (String ngram : ngrams) {
			int currentSize = countWhitspaces(ngram);
			List<String> synonyms = wordnet.getSynonyms(ngram);
			if (synonyms.size() == 0)
				continue;
			if (synonyms.size() != 0 && currentSize > previousNgramSize)
				synsets.clear();
			synsets.addAll(synonyms);
			synsets.addAll(wordnet.getHypernyms(ngram));

			previousNgramSize = currentSize;
		}
		return synsets;

	}
	
}
