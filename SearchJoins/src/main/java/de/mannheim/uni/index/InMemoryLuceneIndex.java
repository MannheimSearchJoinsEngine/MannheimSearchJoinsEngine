package de.mannheim.uni.index;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.math.linear.ArrayRealVector;
import org.apache.commons.math.linear.RealVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.core.SimpleAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Version;

public class InMemoryLuceneIndex {
	
	  /* Indexed, tokenized, stored. */
    public static final FieldType TYPE_STORED = new FieldType();
    public static final String KEY = "Key";
    public static final String CONTENT = "Content";
    private final Set<String> terms = new HashSet<String>();
    private int N = 0;

    static {
        TYPE_STORED.setIndexed(true);
        TYPE_STORED.setTokenized(true);
        TYPE_STORED.setStored(true);
        TYPE_STORED.setStoreTermVectors(true);
        TYPE_STORED.setStoreTermVectorPositions(true);
        TYPE_STORED.freeze();
    }
	
    private IndexWriter writer;
    private IndexReader reader;
    private Directory directory;
    
    public static void main(String[] args) throws IOException {
		InMemoryLuceneIndex i = new InMemoryLuceneIndex();
	
		// from http://stackoverflow.com/questions/1844194/get-cosine-similarity-between-two-documents-in-lucene
		// does not work!
		
		i.beginWrite();
		int d1 = i.addDocument("1", "test test test");
		int d2 = i.addDocument("2", "test test test");
		int d3 = i.addDocument("3", "bla bla bla");
		i.endWrite();
		
		i.beginRead();
		RealVector v1 = i.getWeights(d1);
		RealVector v2 = i.getWeights(d2);
		RealVector v3 = i.getWeights(d3);
		i.endRead();
		
		System.out.println("1<>2: " + i.getCosineSimilarity(v1, v2));
		System.out.println("1<>2: " + i.getCosineSimilarity(v1, v3));
		System.out.println("1<>2: " + i.getCosineSimilarity(v2, v3));
	}
    
	public InMemoryLuceneIndex()
	{

	}

	public void beginWrite() throws IOException
	{
		directory = new RAMDirectory();
        Analyzer analyzer = new SimpleAnalyzer(Version.LUCENE_CURRENT);
        IndexWriterConfig iwc = new IndexWriterConfig(Version.LUCENE_CURRENT,
                analyzer);
        writer = new IndexWriter(directory, iwc);
	}
	
	public void endWrite() throws IOException
	{
		writer.close();
	}
	
	public void beginRead() throws IOException
	{
        if(directory!=null)
        {
        	reader = DirectoryReader.open(directory);
        }
	}
	
	public void endRead() throws IOException
	{
		reader.close();
	}
	
	public int addDocument(String key, String content) throws IOException
	{
	    Document doc = new Document();
	    Field keyField = new Field(KEY, key, TYPE_STORED);
        Field field = new Field(CONTENT, content, TYPE_STORED);
        doc.add(keyField);
        doc.add(field);
        writer.addDocument(doc);
        return N++;
	}
	
    public double getCosineSimilarity(RealVector v1, RealVector v2) {
        double dotProduct = v1.dotProduct(v2);
        System.out.println( "Dot: " + dotProduct);
        System.out.println( "V1_norm: " + v1.getNorm() + ", V2_norm: " + v2.getNorm() );
        double normalization = (v1.getNorm() * v2.getNorm());
        System.out.println( "Norm: " + normalization);
        return dotProduct / normalization;
    }
	
    public RealVector getWeights(int docId)
            throws IOException {
        Terms vector = reader.getTermVector(docId, CONTENT);
        Map<String, Integer> docFrequencies = new HashMap<String, Integer>();
        Map<String, Integer> termFrequencies = new HashMap<String, Integer>();
        Map<String, Double> tf_Idf_Weights = new HashMap<String, Double>();
        TermsEnum termsEnum = null;
        DocsEnum docsEnum = null;


        termsEnum = vector.iterator(termsEnum);
        BytesRef text = null;
        while ((text = termsEnum.next()) != null) {
            String term = text.utf8ToString();
            int docFreq = termsEnum.docFreq();
            docFrequencies.put(term, reader.docFreq( new Term( CONTENT, term ) ));

            docsEnum = termsEnum.docs(null, null);
            while (docsEnum.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                termFrequencies.put(term, docsEnum.freq());
            }

            terms.add(term);
        }

        for ( String term : docFrequencies.keySet() ) {
            int tf = termFrequencies.get(term);
            int df = docFrequencies.get(term);
            double idf = ( 1 + Math.log(N) - Math.log(df) );
            double w = tf * idf;
            tf_Idf_Weights.put(term, w);
        }

        return toRealVector(tf_Idf_Weights);
    }

    RealVector toRealVector(Map<String, Double> map) {
        RealVector vector = new ArrayRealVector(terms.size());
        int i = 0;
        double value = 0;
        for (String term : terms) {

            if ( map.containsKey(term) ) {
                value = map.get(term);
            }
            else {
                value = 0;
            }
            vector.setEntry(i++, value);
        }
        return vector;
    }
}
