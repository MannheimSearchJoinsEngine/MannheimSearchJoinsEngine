package de.mannheim.uni.index;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

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
import org.apache.lucene.search.QueryWrapperFilter;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.Version;

import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class IndexManager implements IValueIndex {
	// public static final String INDEX_DIR = "HeikoTestIndex";//
	// "DBIndex";//"SimpleIndex";//
	// "DBIndex";//
	// "Index";//

	// table index header names
	public static final String ID = "id";
	public static final String tableHeader = "tableHeader";
	public static final String columnHeader = "columnHeader";
	public static final String columnDataType = "columnDataType";
	public static final String tableCardinality = "tableCardinality";
	public static final String columnDistinctValues = "columnDistinctValues";
	public static final String INDEX_ENTRY_VALUE = "value";
	public static final String valueMultiplicity = "valueMultiplicity";
	public static final String FULL_TABLE_PATH = "fullTablePath";
	public static final String IS_PRIMARY_KEY = "isPrimaryKey";
	public static final String ORIGINAL_VALUE = "originalValue";

	private IndexWriter indexWriter = null;
	IndexSearcher indexSearcher = null;
	QueryParser queryParser = null;
	private String indexDir;
	private IndexReader indexReader = null;

	private static HashMap<Thread, QueryParser> queryParserCache = new HashMap<Thread, QueryParser>();

	public QueryParser getQueryParser() {
		getIndexSearcher();
		return queryParser;
	}

	public QueryParser getNewQueryParser() {
		return new QueryParser(Version.LUCENE_46, INDEX_ENTRY_VALUE,
				new StandardAnalyzer(Version.LUCENE_46));
	}

	public String getIndexDir() {
		return indexDir;
	}

	public void setIndexDir(String indexDir) {
		this.indexDir = indexDir;
	}

	private Pipeline pipeline;

	public IndexManager(Pipeline pipeline) {
		this.pipeline = pipeline;
		this.indexDir = pipeline.getIndexLocation();
		// TODO Auto-generated constructor stub
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
				queryParser = new QueryParser(Version.LUCENE_46,
						INDEX_ENTRY_VALUE, new StandardAnalyzer(
								Version.LUCENE_46));

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return this.indexSearcher;
	}

	public IndexSearcher getIndexSearcherFast() {
		if (indexSearcher == null) {

			try {
				File indexDirFile = new File(this.indexDir);
				Directory dir = FSDirectory.open(indexDirFile);
				// Directory dir = MMapDirectory.open(indexDirFile);
				indexReader = DirectoryReader.open(dir);
				indexSearcher = new IndexSearcher(indexReader);

			} catch (IOException ioe) {
				ioe.printStackTrace();
			}
		}
		return this.indexSearcher;
	}

	public Set<String> getTableHeaders() {
		IndexSearcher searcher = getIndexSearcherFast();

		QueryParser qp = getQueryParser();

		IndexReader reader = searcher.getIndexReader();
		HashSet<String> fields = new HashSet<String>();
		HashSet<String> results = new HashSet<String>();
		fields.add(this.columnHeader);

		System.out.println("Index contains " + (reader.maxDoc() - 1)
				+ " documents ....");

		for (int i = 0; i < reader.maxDoc(); i++) {
			try {
				String value = reader.document(i, fields)
						.get(this.columnHeader);

				value = StringNormalizer.clearString(value, true,
						this.pipeline.isSearchStemmedColumnHeaders());

				results.add(value);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		return results;
	}

	public void closeIndexReader() {
		try {
			if (indexReader != null)
				indexReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
		}
	}

	// TODO: implement different index strategies: ngrams, tokens etc
	public synchronized void indexValue(IndexEntry entry) {
		IndexWriter writer = getIndexWriter(false);
		Document doc = new Document();
		doc.add(new IntField(ID, entry.getEntryID(), Field.Store.YES));
		doc.add(new StringField(tableHeader, entry.getTabelHeader(),
				Field.Store.YES));
		doc.add(new StringField(columnHeader, entry.getColumnHeader(),
				Field.Store.YES));
		doc.add(new StringField(columnDataType, entry.getColumnDataType(),
				Field.Store.YES));
		doc.add(new IntField(tableCardinality, entry.getTableCardinality(),
				Field.Store.YES));
		doc.add(new IntField(columnDistinctValues, entry
				.getColumnDistinctValues(), Field.Store.YES));
		doc.add(new IntField(valueMultiplicity, entry.getValueMultiplicity(),
				Field.Store.YES));
		doc.add(new TextField(INDEX_ENTRY_VALUE, entry.getValue(),
				Field.Store.YES));
		doc.add(new StringField(FULL_TABLE_PATH, entry.getFullTablePath(),
				Field.Store.YES));
		doc.add(new StringField(IS_PRIMARY_KEY, Boolean.toString(entry
				.isPrimaryKey()), Field.Store.YES));
		doc.add(new StringField(ORIGINAL_VALUE, entry.getOriginalValue(),
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

	private QueryParser getQueryParserFromCache() {
		QueryParser queryParser = null;
		if (!queryParserCache.containsKey(Thread.currentThread())) {
			queryParser = getNewQueryParser();
			queryParserCache.put(Thread.currentThread(), queryParser);
		} else
			queryParser = queryParserCache.get(Thread.currentThread());

		return queryParser;
	}

	/**
	 * @param valueEntry
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<IndexEntry> searchIndex(IndexEntry valueEntry) {

		// System.out.println("searching for " + valueEntry.getValue());

		// used to remove entries that are added from same table and are exactly
		// the same
		// Map<String, List<String>> passedTables = new HashMap<String,
		// List<String>>();
		Map<String, TreeSet<String>> passedTables = new HashMap<String, TreeSet<String>>();
		List<IndexEntry> entries = new LinkedList<IndexEntry>();

		getIndexSearcherFast();

		// QueryParser queryParser = null;
		QueryParser queryParser = getQueryParserFromCache();
		/*
		 * if(!queryParserCache.containsKey(Thread.currentThread())) {
		 * queryParser = getNewQueryParser();
		 * queryParserCache.put(Thread.currentThread(), queryParser); } else
		 * queryParser = queryParserCache.get(Thread.currentThread());
		 */

		// QueryParser queryParser = getQueryParser();
		// getIndexSearcher();
		String value = valueEntry.getValue();
		String originalKey = "";
		try {
			Query q = null;
			try {
				if (value.equals("") || value.equals(PipelineConfig.NULL_VALUE))
					return entries;
				value = queryParser.escape(value);

				StringBuilder sb = new StringBuilder();

				List<String> terms = StringNormalizer.tokenizeString(value,
						!pipeline.isSearchExactMatches());

				for (int i = 0; i < terms.size(); i++) {
					String term = terms.get(i);
					if (i == 0) {
						originalKey = terms.get(i);
					} else {
						originalKey = originalKey + " " + terms.get(i);
					}

					if (pipeline.isUseEditDistanceSearch())
						term = term + "~" + pipeline.getMaxEditDistance();

					if (i == terms.size() - 1)
						sb.append(term);
					else {
						sb.append(term);
						if (pipeline.isSearchExactMatches())
							sb.append(" AND ");
						else
							sb.append(" OR ");
					}
				}
				value = sb.toString();

				q = queryParser.parse(value);

				System.out.println("searching for " + value);

			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			int numResults = pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;

			BooleanQuery constrainedQuery = new BooleanQuery();

			// add the search query
			constrainedQuery.add(q, BooleanClause.Occur.MUST);

			// if we are looking only for the key values
			// if (pipeline.getKeyidentificationType() ==
			// KeyIdentificationType.singleWithRefineAttrs) {
			TermQuery categoryQuery = new TermQuery(new Term(IS_PRIMARY_KEY,
					"true"));
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
			// }

			if (pipeline.getMaxTableRowsNumber() > 0) {
				// set size of the table
				Query sizeQuery = NumericRangeQuery.newIntRange(
						tableCardinality, 1, 1,
						pipeline.getMaxTableRowsNumber(), true, true);

				constrainedQuery.add(sizeQuery, BooleanClause.Occur.MUST);

			}
			// add tables source constrains
			addSourceContrains(constrainedQuery);

			System.out.println("Query: " + constrainedQuery.toString());

			while (true) {
				try {

					hits = indexSearcher.search(constrainedQuery, numResults).scoreDocs;

					// check if all the scores are the same; if yes, retrieve
					// more documents
					if (hits.length == 0
							|| (hits[0].score != hits[hits.length - 1].score)
							|| numResults > 1000000 || numResults < 0)
						break;
					numResults *= 10;
				} catch (Exception e) {
					e.printStackTrace();
					break;
				}
			}

			System.out.println(" found " + hits.length + " for "
					+ valueEntry.getValue());
			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);

				if (pipeline.isSearchExactMatches()) {
					String foundValue = doc.get(INDEX_ENTRY_VALUE);
					String valueToCheck = originalKey;

					foundValue = StringNormalizer
							.removeCustomStopwords(foundValue);
					valueToCheck = StringNormalizer
							.removeCustomStopwords(valueToCheck);

					// if no custom stopwords are provided, this performs an
					// exact equality check
					if (!valueToCheck.equals(foundValue))
						continue;
				}

				IndexEntry entry = getEntryFromLuceneDoc(doc, hits[i].score);
				if (checkForAllowedSources(entry))
					continue;
				if (pipeline.getKeyidentificationType() == KeyIdentificationType.singleWithRefineAttrs) {
					entry.setRefineAttrs(findRefineAttrsMatches(
							entry.getTabelHeader(), entry.getEntryID(),
							valueEntry.getRefineAttrs()));
				}
				// check if the entry was already found in the same table
				// List<String> passedTablesValue = new ArrayList<String>();
				TreeSet<String> passedTablesValue = new TreeSet<String>();
				if (passedTables.containsKey(entry.getValue())) {
					passedTablesValue = passedTables.get(entry.getValue());
					if (passedTablesValue.contains(entry.getFullTablePath()))
						continue;

				}

				passedTablesValue.add(entry.getFullTablePath());
				passedTables.put(entry.getValue(), passedTablesValue);

				entries.add(entry);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(" returning " + entries.size() + " for "
				+ valueEntry.getValue());
		return entries;
	}

	private boolean checkForAllowedSources(IndexEntry entry) {
		for (String tableSource : pipeline.getTablesToAvoid()) {
			if (entry.getFullTablePath().contains(tableSource))
				return true;
		}
		return false;

	}

	public List<IndexEntry> searchAllValues(String columnHeader) {

		System.out.println("searching for header " + columnHeader);

		List<IndexEntry> entries = new LinkedList<IndexEntry>();

		getIndexSearcher();
		try {
			Query q = null;

			String value = queryParser.escape(columnHeader);

			TermQuery tq = new TermQuery(new Term(columnHeader, value));

			ScoreDoc[] hits = null;

			hits = indexSearcher.search(tq, Integer.MAX_VALUE).scoreDocs;

			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				IndexEntry entry = getEntryFromLuceneDoc(doc, hits[i].score);
				entries.add(entry);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return entries;
	}

	/**
	 * checks whether the given table contains some of the refining attributes
	 * 
	 * @param tableName
	 * @param rowNm
	 * @param refineHeaders
	 * @return
	 */
	@SuppressWarnings("static-access")
	public boolean filterByHeaders(String tableName, int rowNm,
			List<String> refineHeaders) {
		long start = System.currentTimeMillis();
		System.out.println("search refining headers");
		System.out.println(tableName);
		BooleanQuery constrainedQuery = new BooleanQuery();
		// add the table name
		Term term = new Term(FULL_TABLE_PATH, tableName);
		Query query = new TermQuery(term);
		constrainedQuery.add(query, BooleanClause.Occur.MUST);
		// add the rowID
		Query rowIdQuery = NumericRangeQuery.newIntRange(ID, 1, rowNm, rowNm,
				true, true);
		constrainedQuery.add(rowIdQuery, BooleanClause.Occur.MUST);

		ScoreDoc[] hits = null;
		int numResults = 10000;
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

		for (ScoreDoc hit : hits) {
			Document doc = null;
			try {
				doc = indexSearcher.doc(hit.doc);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			for (String header : refineHeaders) {
				if (header.toLowerCase().equals(
						doc.get(columnHeader).toLowerCase())) {
					System.out.println(System.currentTimeMillis() - start);
					return true;
				}
			}

		}
		System.out.println(System.currentTimeMillis() - start);
		return false;

	}

	/**
	 * used to count values from key column
	 * 
	 * @param valuesCount
	 * @param tableName
	 * @param tableName2
	 * @param tablesNames
	 * @throws IOException
	 */
	public void countValuesFromIndex(Map<String, Integer> valuesCount,
			String table, Map<String, List<String>> tablesNames,
			String tableName2) throws Exception {

		getIndexSearcher();

		BooleanQuery constrainedQuery = new BooleanQuery();
		TermQuery categoryQuery = new TermQuery(
				new Term(IS_PRIMARY_KEY, "true"));
		constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
		// tableName = tableName.toLowerCase();
		// get the stream
		// this should not be used in the final version when the field is not
		// analyzed
		// Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
		// TokenStream stream = analyzer.tokenStream(FULL_TABLE_PATH, table);
		// stream.reset();
		// String newValue = "";
		// while (stream.incrementToken()) {
		// newValue = stream.getAttribute(CharTermAttribute.class).toString();
		// Term term = new Term(FULL_TABLE_PATH, newValue);
		// Query query = new TermQuery(term);
		// constrainedQuery.add(query, BooleanClause.Occur.MUST);
		// }
		// stream.close();

		// Term term = new Term(FULL_TABLE_PATH, "datatoindex");
		// Query query = new TermQuery(term);
		// constrainedQuery.add(query, BooleanClause.Occur.MUST);
		// term = new Term(FULL_TABLE_PATH, "petar");
		// query = new TermQuery(term);
		// constrainedQuery.add(query, BooleanClause.Occur.MUST);
		// use it for the new indexes
		Term term = new Term(FULL_TABLE_PATH, table);
		Query query = new TermQuery(term);
		constrainedQuery.add(query, BooleanClause.Occur.MUST);

		ScoreDoc[] hits = null;
		int numResults = 10000;
		while (true) {
			try {

				hits = indexSearcher.search(constrainedQuery, numResults).scoreDocs;
				System.out.println(hits.length);

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
		List<String> valuesFromThisSearch = new ArrayList<String>();

		for (ScoreDoc hit : hits) {
			Document doc = indexSearcher.doc(hit.doc);
			if (!table.equals(doc.get(FULL_TABLE_PATH)))
				continue;

			String value = doc.get(INDEX_ENTRY_VALUE);
			if (valuesFromThisSearch.contains(value))
				continue;

			valuesFromThisSearch.add(value);
			int nm = 1;
			List<String> colTables = new ArrayList<String>();

			if (valuesCount.containsKey(value)) {
				// colTables = tablesNames.get(value);
				// if (colTables.contains(tableName2))
				// continue;
				nm += valuesCount.get(value);
			}
			// colTables.add(tableName2);
			// tablesNames.put(value, colTables);

			valuesCount.put(value, nm);
		}

	}

	/**
	 * adds tables source filter to the query
	 * 
	 * @param constrainedQuery
	 */
	private void addSourceContrains(BooleanQuery constrainedQuery) {
		for (String tableSource : pipeline.getTablesToAvoid()) {
			System.out.println("avoiding sources:" + tableSource);
			Term term = new Term(FULL_TABLE_PATH, "*" + tableSource + "*");
			Query query = new WildcardQuery(term);
			constrainedQuery.add(query, BooleanClause.Occur.MUST_NOT);
		}

	}

	@SuppressWarnings("static-access")
	public List<IndexEntry> findRefineAttrsMatches(String tableName, int rowNm,
			List<IndexEntry> refineAttributes) {
		// holds all of the entries matches
		// Map<IndexEntry, List<IndexEntry>> refineAttrsMatches = new
		// HashMap<IndexEntry, List<IndexEntry>>();
		List<IndexEntry> entries = new ArrayList<IndexEntry>();
		// initialize the searcher, if it is not so far
		getIndexSearcher();
		for (IndexEntry entry : refineAttributes) {
			// List<IndexEntry> entries = new LinkedList<IndexEntry>();

			try {
				Query q = null;
				String value = entry.getValue();
				value = queryParser.escape(value);
				if (value.equals("")
						|| value.equalsIgnoreCase(PipelineConfig.NULL_VALUE))
					// use edit distance
					if (pipeline.isUseEditDistanceSearch())
						value = value + "~";
				q = queryParser.parse(value);

				int numResults = 1;// pipeline.getNmRetrievedDocsFromIndex();
				ScoreDoc[] hits = null;
				QueryWrapperFilter categoryFilter = null;
				// set the table name in the search
				TermQuery categoryQuery = new TermQuery(new Term(tableHeader,
						tableName));
				// set the rowID
				Query rowIdQuery = NumericRangeQuery.newIntRange(ID, 1, rowNm,
						rowNm, true, true);
				BooleanQuery constrainedQuery = new BooleanQuery();
				constrainedQuery.add(q, BooleanClause.Occur.MUST);
				constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
				constrainedQuery.add(rowIdQuery, BooleanClause.Occur.MUST);

				// categoryFilter = new QueryWrapperFilter(categoryQuery);

				while (true) {
					try {
						hits = indexSearcher.search(constrainedQuery,
								categoryFilter, numResults).scoreDocs;
						// check if all the scores are the same; if yes,
						// retrieve
						// more documents
						// if (hits.length == 0
						// || (hits[0].score != hits[hits.length - 1].score)
						// || numResults > 100000 || numResults < 0)
						break;
						// numResults *= 10;
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				for (int i = 0; i < hits.length; i++) {
					Document doc = indexSearcher.doc(hits[i].doc);
					if (!doc.get(INDEX_ENTRY_VALUE).equals(value))
						continue;
					entries.add(getEntryFromLuceneDoc(doc, hits[i].score));
				}

			} catch (Exception e) {
				e.printStackTrace();
			}
			// refineAttrsMatches.put(entry, entries);
		}
		return entries;

	}

	public boolean hasHeader(String tableName, List<String> allowedHeaders) {
		if (allowedHeaders == null)
			return true;

		// initialize the searcher, if it is not so far
		getIndexSearcherFast();

		try {
			int numResults = pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;

			Filter filter = null;
			// set the table name in the search
			TermQuery categoryQuery = new TermQuery(new Term(tableHeader,
					tableName));

			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);

			// filter for the allowed column headers
			if (pipeline.isSearchExactColumnHeaders()) {
				List<Term> terms = new LinkedList<Term>();

				for (String s : allowedHeaders) {
					terms.add(new Term(this.columnHeader, s));
				}

				TermsFilter columnFilter = new TermsFilter(terms);

				CachingWrapperFilter cache = new CachingWrapperFilter(
						columnFilter);

				filter = cache;

				try {
					hits = indexSearcher.search(constrainedQuery, filter,
							numResults).scoreDocs;
				} catch (Exception e) {
					e.printStackTrace();
				}

				return hits.length > 0;
			} else {

				// it's faster to just load any row from the table than to query
				// for all possible headers
				List<IndexEntry> values = getRowValues(tableName, 2);

				for (IndexEntry e : values) {
					String header = StringNormalizer.clearString4FastJoin(
							e.getColumnHeader(), true,
							pipeline.isSearchStemmedColumnHeaders());
					if (allowedHeaders.contains(header))
						return true;
				}

				return false;
			}

		} catch (Exception e) {
			e.printStackTrace();

			return true;
		}
	}

	public List<IndexEntry> getRowValues(String tableName, int rowNm) {
		return getRowValues(tableName, rowNm, null);
	}

	public List<IndexEntry> getRowValues(String tableName, int rowNm,
			List<String> allowedHeaders) {

		List<IndexEntry> entries = new LinkedList<IndexEntry>();
		// initialize the searcher, if it is not so far
		getIndexSearcher();

		try {
			int numResults = pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;
			QueryWrapperFilter categoryFilter = null;
			Filter filter = null;
			// set the table name in the search
			TermQuery categoryQuery = new TermQuery(new Term(tableHeader,
					tableName));
			// set the rowID
			Query rowIdQuery = NumericRangeQuery.newIntRange(ID, 1, rowNm,
					rowNm, true, true);

			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(rowIdQuery, BooleanClause.Occur.MUST);

			// filter for the allowed column headers
			if (allowedHeaders != null && pipeline.isSearchExactColumnHeaders()) {
				List<Term> terms = new LinkedList<Term>();

				for (String s : allowedHeaders) {
					terms.add(new Term(this.columnHeader, s));
				}

				TermsFilter columnFilter = new TermsFilter(terms);

				CachingWrapperFilter cache = new CachingWrapperFilter(
						columnFilter);

				filter = cache;
			}

			// categoryFilter = new QueryWrapperFilter(categoryQuery);

			while (true) {
				try {
					// hits = indexSearcher.search(constrainedQuery,
					// categoryFilter, numResults).scoreDocs;
					hits = indexSearcher.search(constrainedQuery, filter,
							numResults).scoreDocs;
					// check if all the scores are the same; if yes,
					// retrieve
					// more documents
					if (hits.length == 0
							|| (hits[0].score != hits[hits.length - 1].score)
							|| numResults > 100000 || numResults < 0)
						break;
					numResults *= 10;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				entries.add(getEntryFromLuceneDoc(doc, hits[i].score));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return entries;

	}

	public List<IndexEntry> searchIndexForGoldBuilder(IndexEntry keyIndexEntry,
			String tableName) {
		List<IndexEntry> entries = new LinkedList<IndexEntry>();

		getIndexSearcher();

		try {
			Query q = null;
			try {
				String value = keyIndexEntry.getValue();

				value = queryParser.escape(value);
				// use edit distance
				if (pipeline.isUseEditDistanceSearch())
					value = value + "~";
				q = queryParser.parse(value);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			// q = new TermQuery(new Term(INDEX_ENTRY_VALUE, value));
			int numResults = pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;
			// set the table name in the search
			TermQuery categoryQuery = new TermQuery(new Term(tableHeader,
					tableName));

			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(q, BooleanClause.Occur.MUST);
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);

			// search only for key values
			if (pipeline.getKeyidentificationType() == KeyIdentificationType.singleWithRefineAttrs) {
				TermQuery categoryQuery2 = new TermQuery(new Term(
						IS_PRIMARY_KEY, "true"));
				constrainedQuery.add(categoryQuery2, BooleanClause.Occur.MUST);
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
				}
			}

			for (int i = 0; i < hits.length; i++) {
				// System.out.println(hits[i].doc);
				Document doc = indexSearcher.doc(hits[i].doc);

				IndexEntry entry = getEntryFromLuceneDoc(doc, hits[i].score);

				entry.setRefineAttrs(findRefineAttrsMatches(
						entry.getTabelHeader(), entry.getEntryID(),
						keyIndexEntry.getRefineAttrs()));
				/*
				 * for (IndexEntry entryR : entry.getRefineAttrs())
				 * entry.setLuceneScore(entry.getLuceneScore() +
				 * entryR.getLuceneScore());
				 */
				// entries.add(getEntryFromLuceneDoc(doc, hits[i].score));
				entries.add(entry);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
		// sort entries
		return entries;
	}

	public IndexEntry getEntryFromLuceneDoc(Document doc, float score) {
		IndexEntry entry = new IndexEntry();
		entry.setLuceneScore(score);
		entry.setEntryID(Integer.parseInt(doc.get(ID)));
		entry.setColumnDataType(doc.get(columnDataType));
		entry.setColumnHeader(doc.get(columnHeader));
		entry.setColumnDistinctValues(Integer.parseInt(doc
				.get(columnDistinctValues)));
		entry.setTabelHeader(doc.get(tableHeader));
		entry.setTableCardinality(Integer.parseInt(doc.get(tableCardinality)));
		entry.setValueMultiplicity(Integer.parseInt(doc.get(valueMultiplicity)));
		entry.setValue(doc.get(INDEX_ENTRY_VALUE));
		entry.setFullTablePath(doc.get(FULL_TABLE_PATH));
		entry.setOriginalValue(doc.get(ORIGINAL_VALUE));

		if (doc.get(IS_PRIMARY_KEY) != null
				&& Boolean.parseBoolean(doc.get(IS_PRIMARY_KEY))) {
			entry.setPrimaryKey(true);
		} else
			entry.setPrimaryKey(false);
		return entry;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline pipe = Pipeline.getPipelineFromConfigFile("ss",
				"searchJoins.conf");
		IndexManager ma = new IndexManager(pipe);
		IndexEntry en = new IndexEntry();
		en.setValue("germany");
		List<IndexEntry> entries = ma.searchIndex(en);
		for (IndexEntry e : entries)
			System.out.println(e.getFullTablePath());
		System.out.println("found");
		// ma.retrieveRow("Place.csv", 44);

	}

	/**
	 * Runs given query against the index
	 * 
	 * @param query
	 * @param nmResults
	 * @return
	 */
	public List<IndexEntry> runQueryOnIndex(BooleanQuery query, int nmResults) {
		List<IndexEntry> entries = new ArrayList<IndexEntry>();

		getIndexSearcher();

		try {

			int numResults = nmResults;// pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;

			try {
				hits = indexSearcher.search(query, numResults).scoreDocs;
			} catch (Exception e) {
				e.printStackTrace();
			}
			if (hits == null)
				return entries;
			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				entries.add(getEntryFromLuceneDoc(doc, hits[i].score));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		// refineAttrsMatches.put(entry, entries);

		return entries;
	}

	/**
	 * USe for retrieving all values from a given column
	 * 
	 * @param tableName
	 * @param columnName
	 * @return
	 */
	@SuppressWarnings("static-access")
	public List<IndexEntry> getValuesFromColumn(String tableName,
			String columnName, boolean isKey) {

		List<IndexEntry> entries = new ArrayList<IndexEntry>();
		// initialize the searcher, if it is not so far
		getIndexSearcher();

		try {

			int numResults = pipeline.getNmRetrievedDocsFromIndex();// pipeline.getNmRetrievedDocsFromIndex();
			ScoreDoc[] hits = null;

			// set the table name in the search
			TermQuery categoryQuery = new TermQuery(new Term(FULL_TABLE_PATH,
					tableName));
			TermQuery columnQuery = new TermQuery(new Term(columnHeader,
					columnName));
			// get the key column, regardless the column header
			if (isKey)
				columnQuery = new TermQuery(new Term(IS_PRIMARY_KEY, "true"));

			BooleanQuery constrainedQuery = new BooleanQuery();
			constrainedQuery.add(categoryQuery, BooleanClause.Occur.MUST);
			constrainedQuery.add(columnQuery, BooleanClause.Occur.MUST);

			// categoryFilter = new QueryWrapperFilter(categoryQuery);

			while (true) {
				try {
					// System.out.println("retrieving values for " + tableName
					// + " " + numResults);
					hits = indexSearcher.search(constrainedQuery, numResults).scoreDocs;
					// check if all the scores are the same; if yes,
					// retrieve
					// more documents
					if (hits.length == 0
							|| (hits[0].score != hits[hits.length - 1].score)
							|| numResults > 100000 || numResults < 0)
						break;
					numResults *= 10;
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				entries.add(getEntryFromLuceneDoc(doc, hits[i].score));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		// refineAttrsMatches.put(entry, entries);

		return entries;

	}
}
