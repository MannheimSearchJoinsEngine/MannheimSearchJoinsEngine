package de.mannheim.uni.schemamatching;

import java.util.ArrayList;
import java.util.List;

import de.mannheim.uni.lod.SPARQLEndpointQueryRunner;
import de.mannheim.uni.lod.WordnetAPI;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;

public class SimilarTermsGenerator {

	Pipeline p;
	WordnetAPI wordnet;
	
	public static final String DBPEDIA_URI_PREFIX = "http://dbpedia.org/ontology/";
	public static final String YAGO_URI_PREFIX = "http://yago-knowledge.org/resource/";
	
	public SimilarTermsGenerator(Pipeline pipeline)
	{
		p = pipeline;
		
		wordnet = WordnetAPI.getInstance(pipeline
				.getWordnetDictionaryPath());
	}
	
	public List<String> getWordnetTerms(String term, boolean getSynonyms, boolean getHypernyms, boolean getHyponyms)
	{
		String value = StringNormalizer.clearString(term, false);
		
		ArrayList<String> terms = new ArrayList<String>();
		
		if(getSynonyms)
			terms.addAll(wordnet.getSynonyms(value));
		
		if(getHypernyms)
			terms.addAll(wordnet.getHypernyms(value));
		
		if(getHyponyms)
			terms.addAll(wordnet.getHyponyms(value));
		
		return terms;
	}
	
	public List<String> getDBPediaSuperClasses(String term)
	{
		String value = term.replaceFirst(Character.toString(term
				.charAt(0)), Character.toString(Character
				.toUpperCase(term.charAt(0))));
		
		SPARQLEndpointQueryRunner queryRunner = new SPARQLEndpointQueryRunner(
				SPARQLEndpointQueryRunner.DBPEDIA_ENDPOINT);
		queryRunner.setPageSize(10000);

		List<String> superClasses = queryRunner
				.getSuperClasses(DBPEDIA_URI_PREFIX + value);

		return superClasses;
	}
	
	public List<String> getYagoSuperClasses(String term)
	{
		SPARQLEndpointQueryRunner queryRunner = new SPARQLEndpointQueryRunner(
				SPARQLEndpointQueryRunner.YAGO_ENDPOINT);
		queryRunner.setPageSize(10000);

		//String ClassName = queryRunner.getClassFromLabel(term);
		
		List<String> superClasses = queryRunner
				.getSuperClasses(YAGO_URI_PREFIX + term);

		return superClasses;
	}
}
