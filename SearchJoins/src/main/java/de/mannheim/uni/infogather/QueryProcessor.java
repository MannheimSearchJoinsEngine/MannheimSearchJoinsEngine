package de.mannheim.uni.infogather;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.mongodb.DBObject;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.datafusion.DataFuser;
import de.mannheim.uni.index.infogather.AttributesIndexManager;
import de.mannheim.uni.index.infogather.KeyIndexManager;
import de.mannheim.uni.model.ColumnIndexEntry;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.mongodb.MongoDBReader;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.searchjoin.SearchJoin;
import de.mannheim.uni.statistics.DataLogger;
import de.mannheim.uni.statistics.Timer;
import edu.cmu.graphchi.apps.randomwalks.BerkeleyManager;

public class QueryProcessor {

	private Pipeline pipe;
	private Set<String> relevantTables;
	private DataLogger log;

	public QueryProcessor(Pipeline pipeline) {
		pipe = pipeline;
		log = new DataLogger(pipeline);
	}

	public Table AugmentTable(String table, String attributeName) {
		ConvertFileToTable fileToTable = new ConvertFileToTable(pipe);
		Table t = fileToTable.readTable(ReadTableType.search, table);
		
		return AugmentTable(t, attributeName);
	}
	
	public Table AugmentTable(Table table, String attributeName) {
		Timer t = new Timer("InfoGather.AugmentTable");
		// Query Time Processing: The query time processing can be abstracted in
		// three main steps. The details of each step depends on the operation.
		// We provide those details for each operation in Section 5.

		// Q1: Identify the seed tables:
		// We leverage the WIK, WIKV and WIA indexes to identify the seed tables
		// and compute their DMA scores.
		Map<String, Map<Integer, Double>> seedTables = GetSeedTables(table, attributeName);

		// Q2: Compute the TSP scores: We compute the preference vector β by
		// plugging the DMA matching scores in Eq. 6. According to Theorem 1, we
		// can use ⃗ β and the stored PPR vectors of each table to compute the
		// TSP score for each web table. Note that only the seed tables have
		// non-zero entries in ⃗ β. Accordingly, we need to retrieve the PPR
		// vectors of only the seed tables using the T2PPV index. Furthermore,
		// we do not need to compute TSP scores for all web tables in the
		// retrieved PPR vectors. We need to compute it only for the tables that
		// could be used in the aggregation step: the one that have at least one
		// key overlapping with the query table. We refer to them as relevant
		// tables. These can be identified efficiently by invoking WIK(Q). These
		// two optimizations are important to compute the TSP scores efficiently
		Map<String, Map<Integer, Double>> tsp = ComputeTSP(seedTables);

		// Q3: Aggregate and select values: In this step, we collect the
		// predictions provided by the relevant web tables T along with the
		// scores SHol(T). The predictions are then processed, the scores are
		// aggregated and the final predictions are selected according to the
		// operation.
		Table result = AggregateAndSelectValues(table, tsp);
		
		t.stop();
		
		System.out.println(t.toString());
		
		return result;
	}

	/**
	 * Q1: Identify the seed tables: We leverage the WIK, WIKV and WIA indexes
	 * to identify the seed tables and compute their DMA scores.
	 * 
	 * @param table
	 * @return
	 */
	protected Map<String, Map<Integer, Double>> GetSeedTables(Table table,
			String attributeName) {
		Timer tim = new Timer("InfoGather.GetSeedTables");
		
		KeyIndexManager im = new KeyIndexManager(pipe);
		AttributesIndexManager aim = new AttributesIndexManager(pipe);

		TableColumn key = null;
		for (TableColumn c : table.getColumns())
			if (c.isKey()) {
				key = c;
				break;
			}

		// Augmentation-By-Attribute: if the attribute name is given, use
		// AttributesIndexManager to get all tables that contain this attribute
		// then use only the results that were returned from both indexes
		List<ColumnIndexEntry> colResults = null;

		if (attributeName != null)
			colResults = aim.searchIndex(attributeName);

		// keep track of the number of matched values for each table from the
		// search results <Table Name, Overlap>
		Map<String, Map<Integer, Integer>> overlapCount = new HashMap<String, Map<Integer, Integer>>();

		// while determining the seed tables, we can also generate the list of
		// relevant tables for the TSP computation
		relevantTables = new HashSet<String>();

		// for every key value
		for (Entry<Integer, String> keyValue : key.getValues().entrySet()) {
			// search key-index for the current value
			List<IndexEntry> results = im.searchIndex(keyValue.getValue());

			// add all tables that were found to the list of relevant tables
			for (IndexEntry e : results)
				relevantTables.add(e.getTabelHeader());

			Map<IndexEntry, ColumnIndexEntry> map = null;
			
			if (attributeName == null)
			{
				map = new HashMap<IndexEntry, ColumnIndexEntry>();
				
				for (IndexEntry e : results) {
					map.put(e, null);
				}
			}
			else
				map = getIntersection(results, colResults);
			
			
			
			// add count for each table that was found
			for (Entry<IndexEntry, ColumnIndexEntry> e : map.entrySet()) {
				String k = e.getKey().getTabelHeader();
				int col = 0; 
				
				if(e.getValue()!=null)
					col = e.getValue().getColumnID();
				
				overlapCount.put(k, new HashMap<Integer, Integer>());
				
				if (overlapCount.get(k).containsKey(col))
					overlapCount.get(k).put(col, overlapCount.get(k).get(col) + 1);
				else
					overlapCount.get(k).put(col, 1);
			}
		}

		// calculate DMA scores
		Map<String, Map<Integer, Double>> scores = new HashMap<String, Map<Integer, Double>>();

		MongoDBReader reader = new MongoDBReader();

		for (String tableName : overlapCount.keySet()) {
			// get rowCount from tblMeta object
			int rowCount = reader.getRowNumberForFile(tableName);
			Map<Integer, Double> m = new HashMap<Integer, Double>();
			scores.put(tableName, m);
			
			for(int col : overlapCount.get(tableName).keySet())
			{
				double score = (double) overlapCount.get(tableName).get(col)
						/ (double) Math.min(key.getTotalSize(), rowCount);
	
				m.put(col, score);
			}
		}
		
		log.logMapMap(scores, "seed_tables_dma_scores");

		tim.stop();
		return scores;
	}

	/**
	 * Q2: Compute the TSP scores: We compute the preference vector β by
	 * plugging the DMA matching scores in Eq. 6. According to Theorem 1, we can
	 * use ⃗ β and the stored PPR vectors of each table to compute the TSP score
	 * for each web table. Note that only the seed tables have non-zero entries
	 * in ⃗ β. Accordingly, we need to retrieve the PPR vectors of only the seed
	 * tables using the T2PPV index. Furthermore, we do not need to compute TSP
	 * scores for all web tables in the retrieved PPR vectors. We need to
	 * compute it only for the tables that could be used in the aggregation
	 * step: the one that have at least one key overlapping with the query
	 * table. We refer to them as relevant tables. These can be identified
	 * efficiently by invoking WIK(Q). These two optimizations are important to
	 * compute the TSP scores efficiently
	 * 
	 * @param seedTables
	 * @return
	 */
	protected Map<String, Map<Integer, Double>> ComputeTSP(Map<String, Map<Integer, Double>> seedTables) {
		Timer tim = new Timer("InfoGater.ComputeTSP");
		// initialize the berkeleyDB
		BerkeleyManager bManager = new BerkeleyManager(pipe.getT2pprIndexPath());
		MongoDBReader reader = new MongoDBReader();
		
		double sumOfScores = 0.0;

		for(String table : seedTables.keySet())
			for(int col : seedTables.get(table).keySet())
				sumOfScores += seedTables.get(table).get(col);
		
		Map<String, Map<Integer, Double>> beta = new HashMap<String, Map<Integer,Double>>();
		
		for(String table : seedTables.keySet())
		{
			Map<Integer, Double> m = new HashMap<Integer, Double>();
			beta.put(table, m);
			
			for(int col : seedTables.get(table).keySet())
			{
				m.put(col, seedTables.get(table).get(col) / sumOfScores);
			}
		}

		// get the relevant tables, i.e. all tables that have at least one
		// overlapping key with the query table
		// if we had used an attribute name for finding the seed tables,
		// relevant tables would be many more than the seed tables
		// List<String> relevantTables = new
		// ArrayList<String>(seedTables.keySet());
		// this has already been done in GetSeedTables

		// Calculate TSP (topic sensitive pagerank) scores
		// Eq. 7

		Map<String, Map<Integer, Double>> tsp = new HashMap<String, Map<Integer, Double>>();

		for (String v : relevantTables) {
			 // get number of columns in current relevant table
			int relCols = reader.getColumnNumberForFile(v);

			HashMap<Integer, Double> m = new HashMap<Integer, Double>();
			tsp.put(v, m);
			
			for (int i = 0; i < relCols; i++) {
				double value = 0.0;
				
				for (String u : seedTables.keySet()) {
					// get number of columns in current seed table
					int seedCols = reader.getColumnNumberForFile(u); 

					for (int j = 0; j < seedCols; j++) {
						// check if the column index represents a virtual table (key columns do not create virtual tables)
						if(beta.get(u).containsKey(j))
						{
							// get graph ids from lookup table
							int uI = reader.getGraphIndexNumberForFile(v, i);
							int vI = reader.getGraphIndexNumberForFile(u, j);
							
							Timer tBer = Timer.getNamed("Berkley get PPR", tim);
							value += beta.get(u).get(j) * bManager.getPPRforPair(uI, vI);// ppr.get(u).get(v);
							tBer.stop();
						}
					}
				}
				
				// set the TSP for the virtual table (v,i) = table v, column i
				m.put(i, value);
			}
		}

		log.logMapMap(tsp, "relevant_tables_tsp_scores");
		
		tim.stop();
		return tsp;
	}

	/**
	 * Q3: Aggregate and select values: In this step, we collect the predictions
	 * provided by the relevant web tables T along with the scores SHol(T). The
	 * predictions are then processed, the scores are aggregated and the final
	 * predictions are selected according to the operation.
	 * 
	 * @param table
	 * @param tsp
	 * @return
	 */
	protected Table AggregateAndSelectValues(Table table,
			Map<String, Map<Integer, Double>> tsp) {
		Timer tim = new Timer("InfoGather.AggregateAndSelectValues");
		
		// now actually load the values of all tables in the tsp-map from MongoDB
		SearchJoin sJoins = new SearchJoin(pipe);
		List<JoinResult> joinResults = new ArrayList<JoinResult>( sJoins
				.findJoinsForColumn(table.getCompaundKeyColumns().get(0),
						table, tsp).values() );
		
		// start the data-fusion
		DataFuser fuser = new DataFuser(pipe);
		Table fusedTable = fuser.fuseQueryTableWithEntityTables(table, joinResults,
				tsp);

		log.logTable(table, "infogather_fused");
		
		tim.stop();
		// return the fused and cleaned table
		return fusedTable;
	}

	/**
	 * returns those elements of list1 that have a matching entry in list2
	 * 
	 * @param list1
	 * @param list2
	 * @return
	 */
	protected Map<IndexEntry, ColumnIndexEntry> getIntersection(List<IndexEntry> list1,
			List<ColumnIndexEntry> list2) {
		//List<IndexEntry> result = new LinkedList<IndexEntry>();
		HashMap<IndexEntry, ColumnIndexEntry> result = new HashMap<IndexEntry, ColumnIndexEntry>();

		for (IndexEntry ie : list1) {
			for (ColumnIndexEntry ie2 : list2) {
				if (ie2.getTableHeader().equals(ie.getTabelHeader())) {
					//result.add(ie);
					result.put(ie, ie2);
					break;
				}
			}
		}

		return result;
	}

	public static void main(String[] args) {
		BerkeleyManager bManager = new BerkeleyManager(
				"C:\\Users\\petar\\Documents\\GitHub\\graphchi-java\\berkeleydb");
		System.out.println(bManager.getPPRforPair(0, 0));

	}
}
