package de.mannheim.uni.index;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermsFilter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.CachingWrapperFilter;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;

/**
 * @author petar
 * 
 */
public class AttributesIndexManager {

	// table index header names
	public static final String tableHeader = "tableHeader";
	public static final String columnHeader = "value";
	public static final String columnDataType = "columnDataType";
	public static final String tableCardinality = "tableCardinality";
	public static final String columnDistinctValues = "columnDistinctValues";
	public static final String columnOriginalHeader = "columnOriginalHeader";
	public static final String columnIndex = "columnindex";
	public static final String FULL_TABLE_PATH = "fullTablePath";

	private IndexWriter indexWriter = null;
	IndexSearcher indexSearcher = null;
	QueryParser queryParser = null;
	private String indexDir;
	private IndexReader indexReader = null;
	private Pipeline pipeline;

	public IndexReader getIndexReader() {
		return indexReader;
	}

	public AttributesIndexManager(Pipeline pipeline) {

		this.pipeline = pipeline;
		this.indexDir = pipeline.getHeadersIndexLocation();

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
				queryParser = new QueryParser(Version.LUCENE_46, columnHeader,
						new StandardAnalyzer(Version.LUCENE_46));

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return this.indexSearcher;
	}

	public synchronized void indexValue(Object entryO) {
		ColumnIndexEntry entry = (ColumnIndexEntry) entryO;
		IndexWriter writer = getIndexWriter(false);
		Document doc = new Document();
		doc.add(new StringField(tableHeader, entry.getTableHeader(),
				Field.Store.YES));
		// this is the only analyzed field
		doc.add(new TextField(columnHeader, entry.getColumnHeader(),
				Field.Store.YES));
		doc.add(new StringField(columnDataType, entry.getColumnDataType(),
				Field.Store.YES));
		doc.add(new IntField(tableCardinality, entry.getTableCardinality(),
				Field.Store.YES));
		doc.add(new IntField(columnDistinctValues, entry
				.getColumnDistinctValues(), Field.Store.YES));
		doc.add(new IntField(columnIndex, entry.getColumnID(), Field.Store.YES));
		doc.add(new StringField(columnOriginalHeader, entry
				.getColumnOrignalHeader(), Field.Store.YES));

		doc.add(new StringField(FULL_TABLE_PATH, entry.getTableFullPath(),
				Field.Store.YES));
		// doc.add(new TextField(ORIGINAL_VALUE, entry.getOriginalValue(),
		// Field.Store.YES));
		try {
			writer.addDocument(doc);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * @param valueEntry
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<ColumnIndexEntry> searchIndex(String valueO) {
		String value = (String) valueO;

		List<ColumnIndexEntry> entries = new LinkedList<ColumnIndexEntry>();

		getIndexSearcher();

		try {
			Query q = null;
			try {
				if (value.equals("")
						|| value.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
					return entries;
				value = queryParser.escape(value);

				// use edit distance
				if (pipeline.isUseEditDistanceSearch())
					value = value + "~";
				q = queryParser.parse(value);
				// if (pipeline.isSearchExactMatches()) {
				// q = null;
				// q = new TermQuery(new Term(INDEX_ENTRY_VALUE, value));
				// }

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			int numResults = pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;

			BooleanQuery constrainedQuery = new BooleanQuery();
			// add the search query
			constrainedQuery.add(q, BooleanClause.Occur.MUST);

			if (pipeline.getMaxTableRowsNumber() > 0) {
				// set size of the table
				Query sizeQuery = NumericRangeQuery.newIntRange(
						tableCardinality, 1, 1,
						pipeline.getMaxTableRowsNumber(), true, true);

				constrainedQuery.add(sizeQuery, BooleanClause.Occur.MUST);
			}
			while (true) {
				try {

					hits = indexSearcher.search(constrainedQuery, numResults).scoreDocs;

					// check if all the scores are the same; if yes, retrieve
					// more documents
					if (hits.length == 0
							|| (hits[0].score != hits[hits.length - 1].score)
							|| numResults > 100000 || numResults < 0)
						break;
					numResults *= 10;
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}

			System.out.println(" found " + hits.length + " for " + value);
			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				if (pipeline.isSearchExactMatches()
						&& !doc.get(columnHeader).equals(value))
					break;
				ColumnIndexEntry entry = (ColumnIndexEntry) getEntryFromLuceneDocColumn(
						doc, hits[i].score);

				entries.add(entry);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return entries;
	}

	public static ColumnIndexEntry getEntryFromLuceneDocColumn(Document doc,
			float score) {
		ColumnIndexEntry entry = new ColumnIndexEntry();
		entry.setLuceneScore(score);
		entry.setColumnDataType(doc.get(columnDataType));
		entry.setColumnHeader(doc.get(columnHeader));
		entry.setColumnDistinctValues(Integer.parseInt(doc
				.get(columnDistinctValues)));
		entry.setTableHeader(doc.get(tableHeader));
		entry.setTableCardinality(Integer.parseInt(doc.get(tableCardinality)));
		entry.setColumnOrignalHeader(doc.get(columnOriginalHeader));
		try {
			entry.setColumnID(Integer.parseInt(doc.get(columnIndex)));
		} catch (Exception e) {
			System.out.println("no id found");
			System.out.println(entry.getColumnOrignalHeader());
		}
		entry.setTableFullPath(doc.get(FULL_TABLE_PATH));
		return entry;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		// Pipeline pipe = Pipeline.getPipelineFromConfigFile("ss",
		// "searchJoins.conf");
		// System.out.println("searching");
		// AttributesIndexManager ma = new AttributesIndexManager(pipe);
		//
		// // List<String> allowedTables = new ArrayList<String>();
		// // allowedTables.add("AcademicJournal.csv.gz");
		// // ma.searchForTablesByHeader("label", allowedTables);
		// List<ColumnIndexEntry> entries = ma.searchIndex("industry");
		// for (ColumnIndexEntry e : entries) {
		// if (e.getTableFullPath().contains("BTC")) {
		// System.out.println(e.getColumnOrignalHeader());
		// System.out.println(e.getColumnHeader());
		// System.out.println(e.getTableFullPath());
		// }
		// }
		// System.out.println("found");
		// // ma.retrieveRow("Place.csv", 44);

		// generateHeadersList("WikiHeaders.txt", "/datasets/wikitables/");
		generateHeadersList(args[0], args[1]);

	}

	public static void generateHeadersList(String fileName, String pattern)
			throws IOException {
		Writer writer = null;
		try {

			writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(fileName, true), "utf-8"));
		} catch (Exception e) {

		}
		Pipeline pipe = Pipeline.getPipelineFromConfigFile("ss",
				"searchJoins.conf");
		System.out.println("searching");
		AttributesIndexManager ma = new AttributesIndexManager(pipe);
		ma.getIndexSearcher();
		// create IndexReader
		for (int i = 0; i < ma.getIndexReader().maxDoc(); i++) {
			// if (ma.getIndexReader().isDeleted(i))
			// continue;

			Document doc = ma.getIndexReader().document(i);

			ColumnIndexEntry entry = (ColumnIndexEntry) getEntryFromLuceneDocColumn(
					doc, 0);
			if (entry.getTableFullPath().contains(pattern)) {
				String toWriteStr = entry.getColumnOrignalHeader() + "\t"
						+ entry.getColumnHeader() + "\t"
						+ entry.getTableFullPath() + "\n";
				writer.write(toWriteStr);
				System.out.println(toWriteStr);
			}

		}
		writer.close();
	}

	/**
	 * Searches for tables that contains any od the allowedHeaders in the
	 * allowedTables
	 * 
	 * @param headers
	 * @param allowedTables
	 * @return
	 */
	public List<String> findTablesByHeaders(List<String> headers,
			List<String> allowedTables) {
		final List<String> validTables = Collections
				.synchronizedList(new ArrayList<String>());

		getIndexSearcher();

		final List<String> allowedTablesCopy = allowedTables;
		try {
			new Parallel<String>().foreach(headers, new Consumer<String>() {

				public void execute(String parameter) {

					List<String> lst = searchForTablesByHeader(parameter,
							allowedTablesCopy);

					System.out.println("Found " + lst.size()
							+ " tables for header " + parameter);

					validTables.addAll(lst);
				}
			});
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return validTables;
	}

	private List<String> searchForTablesByHeader(String header,
			List<String> allowedTables) {
		List<String> foundTables = new ArrayList<String>();

		int numResults = pipeline.getNmRetrievedDocsFromIndex();
		ScoreDoc[] hits = null;

		// set the filter
		// filter for the allowed table headers
		Filter filter = null;

		if (allowedTables != null && !allowedTables.isEmpty()) {
			List<Term> terms = new LinkedList<Term>();

			for (String s : allowedTables) {
				terms.add(new Term(this.tableHeader, s));
			}

			TermsFilter columnFilter = new TermsFilter(terms);
			CachingWrapperFilter cache = new CachingWrapperFilter(columnFilter);
			filter = cache;
		}

		// set the header name in the search
		BooleanQuery constrainedQuery = new BooleanQuery();

		if (pipeline.isSearchExactColumnHeaders()) {
			TermQuery categoryQuery = new TermQuery(new Term(columnHeader,
					header));
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);

		} else {
			getIndexSearcher();
			String value = header;
			Query q = null;
			try {
				value = queryParser.escape(value);
				q = queryParser.parse(value);
			} catch (Exception e) {
				e.printStackTrace();
			}
			constrainedQuery.add(q, BooleanClause.Occur.MUST);

		}
		try {
			hits = indexSearcher.search(constrainedQuery, filter, numResults).scoreDocs;
			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				String foundHeader = doc.get(columnHeader);
				if (pipeline.isSearchExactColumnHeaders()
						&& !foundHeader.equals(header))
					continue;
				String tableHeader = doc.get(this.tableHeader);
				if (allowedTables.contains(tableHeader)) {
					foundTables.add(tableHeader);
					// allowedTables.remove(allowedTables);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return foundTables;
	}
}
