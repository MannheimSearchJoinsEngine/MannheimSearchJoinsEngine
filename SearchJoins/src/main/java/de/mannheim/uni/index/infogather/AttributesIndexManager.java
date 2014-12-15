package de.mannheim.uni.index.infogather;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;

import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.utils.PipelineConfig;

/**
 * @author petar
 * 
 */
public class AttributesIndexManager extends IndexManager<ColumnIndexEntry> {

	// table index header names
	public static final String tableHeader = "tableHeader";
	public static final String columnHeader = "value";
	public static final String columnDataType = "columnDataType";
	public static final String tableCardinality = "tableCardinality";
	public static final String columnDistinctValues = "columnDistinctValues";
	public static final String columnOriginalHeader = "columnOriginalHeader";
	public static final String columnIndex = "columnindex";

	public AttributesIndexManager(Pipeline pipeline) {
		super(pipeline);
		this.indexDir = pipeline.getInfoGatherAttributeIndexLocation();
	}

	@Override
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
	public List<ColumnIndexEntry> searchIndex(Object valueO) {
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
				ColumnIndexEntry entry = (ColumnIndexEntry) getEntryFromLuceneDoc(
						doc, hits[i].score);

				entries.add(entry);
			}

		} catch (IOException e) {
			e.printStackTrace();
		}

		return entries;
	}

	@Override
	public Object getEntryFromLuceneDoc(Document doc, float score) {
		ColumnIndexEntry entry = new ColumnIndexEntry();
		entry.setLuceneScore(score);
		entry.setColumnDataType(doc.get(columnDataType));
		entry.setColumnHeader(doc.get(columnHeader));
		entry.setColumnDistinctValues(Integer.parseInt(doc
				.get(columnDistinctValues)));
		entry.setTableHeader(doc.get(tableHeader));
		entry.setTableCardinality(Integer.parseInt(doc.get(tableCardinality)));
		entry.setColumnOrignalHeader(doc.get(columnOriginalHeader));
		entry.setColumnID(Integer.parseInt(doc.get(columnIndex)));
		return entry;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Pipeline pipe = new Pipeline("name",
				"C:\\Users\\petar\\workspace1\\SearchJoinTmpData\\DBIndex");
		AttributesIndexManager ma = new AttributesIndexManager(pipe);
		// ma.retrieveRow("Place.csv", 44);

	}

}
