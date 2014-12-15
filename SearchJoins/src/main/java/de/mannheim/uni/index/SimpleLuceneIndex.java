package de.mannheim.uni.index;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.math.linear.RealVector;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

public class SimpleLuceneIndex {
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
   
	public void beginWrite(String indexDir) throws IOException
	{
		boolean create = false;
		File indexDirFile = new File(indexDir);
		if (indexDirFile.exists() && indexDirFile.isDirectory()) {
			create = false;
		}

		directory = FSDirectory.open(indexDirFile);
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		IndexWriterConfig iwc = new IndexWriterConfig(
				Version.LUCENE_46, analyzer);

		if (create) {
			// Create a new index in the directory, removing any
			// previously indexed documents:
			iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
		}

		writer = new IndexWriter(directory, iwc);
		writer.getConfig().setRAMBufferSizeMB(1024);
	}
	
	public void endWrite() throws IOException
	{
		writer.close();
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
}
