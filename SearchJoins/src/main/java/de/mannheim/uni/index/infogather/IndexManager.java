package de.mannheim.uni.index.infogather;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.pipelines.Pipeline;

public abstract class IndexManager<IndexEntryType> {

	public static final String VALUE = "value";

	protected IndexWriter indexWriter = null;
	protected IndexSearcher indexSearcher = null;
	protected QueryParser queryParser = null;
	protected String indexDir;
	protected IndexReader indexReader = null;

	public QueryParser getQueryParser() {
		getIndexSearcher();
		return queryParser;
	}

	public String getIndexDir() {
		return indexDir;
	}

	public void setIndexDir(String indexDir) {
		this.indexDir = indexDir;
	}

	protected Pipeline pipeline;

	public IndexManager(Pipeline pipeline) {
		this.pipeline = pipeline;
	}

	public IndexWriter getIndexWriter(boolean create) {
		if (indexWriter == null) {
			try {
				long start = System.currentTimeMillis();

				File indexDirFile = new File(this.indexDir);
				if (indexDirFile.exists() && indexDirFile.isDirectory()) {
					create = false;
				}

				Directory dir = FSDirectory.open(indexDirFile);
				Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
				IndexWriterConfig iwc = new IndexWriterConfig(
						Version.LUCENE_46, analyzer);

				if (create) {
					// Create a new index in the directory, removing any
					// previously indexed documents:
					iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
				}

				indexWriter = new IndexWriter(dir, iwc);
				// IndexWriterConfig conf = new IndexWriterConfig(
				// Version.LUCENE_46, analyzer);
				indexWriter.getConfig().setRAMBufferSizeMB(1024);
				long end = System.currentTimeMillis();

				pipeline.getLogger().info(
						"Time openinng the index: "
								+ ((double) (end - start) / 1000));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return indexWriter;
	}

	public void closeIndexWriter() {
		if (indexWriter != null) {
			try {
				indexWriter.commit();
				indexWriter.close();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}
	}

	public int getNmDocs() {
		return getIndexSearcher().getIndexReader().numDocs();
	}

	public IndexSearcher getIndexSearcher() {
		if (indexSearcher == null) {

			try {
				File indexDirFile = new File(this.indexDir);
				Directory dir = FSDirectory.open(indexDirFile);
				indexReader = DirectoryReader.open(dir);
				indexSearcher = new IndexSearcher(indexReader);
				queryParser = new QueryParser(Version.LUCENE_46, VALUE,
						new StandardAnalyzer(Version.LUCENE_46));

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return this.indexSearcher;

	}

	public void closeIndexReader() {
		try {
			if (indexReader != null)
				indexReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}

	public synchronized void indexValue(Object entry) {

	}

	public List<IndexEntryType> searchIndex(Object value) {
		return null;
	}

	public Object getEntryFromLuceneDoc(Document doc, float score) {
		return null;
	}
}
