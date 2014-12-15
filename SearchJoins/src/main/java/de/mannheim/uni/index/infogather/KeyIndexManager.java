package de.mannheim.uni.index.infogather;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
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

import com.hp.hpl.jena.sparql.function.library.pi;

import de.mannheim.uni.TableProcessor.TableManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class KeyIndexManager extends IndexManager<IndexEntry> {

	// table index header names
	public static final String ID = "id";
	public static final String tableHeader = "tableHeader";
	public static final String columnHeader = "columnHeader";
	public static final String columnDataType = "columnDataType";
	public static final String tableCardinality = "tableCardinality";
	public static final String columnDistinctValues = "columnDistinctValues";
	public static final String INDEX_ENTRY_VALUE = "value";
	public static final String valueMultiplicity = "valueMultiplicity";
	public static final String ORIGINAL_VALUE = "originalValue";

	public KeyIndexManager(Pipeline pipeline) {
		super(pipeline);
		this.indexDir = pipeline.getInfoGatherKeyIndexLocation();
		// TODO Auto-generated constructor stub
	}

	@Override
	public synchronized void indexValue(Object entryO) {
		IndexEntry entry = (IndexEntry) entryO;

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

	/**
	 * @param valueEntry
	 * @return
	 */
	@SuppressWarnings("static-access")
	@Override
	public List<IndexEntry> searchIndex(Object valueEntryO) {
		String valueEntry = (String) valueEntryO;

		System.out.println("searching for " + valueEntry);

		// used to remove entries that are added from same table and are exactly
		// the same
		Map<String, List<String>> passedTables = new HashMap<String, List<String>>();
		List<IndexEntry> entries = new LinkedList<IndexEntry>();

		getIndexSearcher();
		String value = valueEntry;
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

			System.out.println(" found " + hits.length + " for " + valueEntry);
			for (int i = 0; i < hits.length; i++) {
				Document doc = indexSearcher.doc(hits[i].doc);
				if (pipeline.isSearchExactMatches()
						&& !doc.get(INDEX_ENTRY_VALUE).equals(value))
					break;
				IndexEntry entry = (IndexEntry) getEntryFromLuceneDoc(doc,
						hits[i].score);

				entries.add(entry);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return entries;
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
				IndexEntry entry = (IndexEntry) getEntryFromLuceneDoc(doc,
						hits[i].score);
				entries.add(entry);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return entries;
	}

	@Override
	public Object getEntryFromLuceneDoc(Document doc, float score) {
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
		entry.setOriginalValue(doc.get(ORIGINAL_VALUE));
		entry.setFullTablePath(doc.get(tableHeader));
		entry.setPrimaryKey(true);

		return entry;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline pipe = new Pipeline("name",
				"C:\\Users\\petar\\workspace1\\SearchJoinTmpData\\DBIndex");
		KeyIndexManager ma = new KeyIndexManager(pipe);
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
				entries.add((IndexEntry) getEntryFromLuceneDoc(doc,
						hits[i].score));
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
		// refineAttrsMatches.put(entry, entries);

		return entries;
	}

}
