package de.mannheim.uni.main;

import java.util.HashMap;
import java.util.Map;

import de.mannheim.uni.IO.ConvertFileToTable.ReadTableType;
import de.mannheim.uni.index.ParallelIndexer;
import de.mannheim.uni.index.Indexer;
import de.mannheim.uni.index.ParallelIndexerIO;
import de.mannheim.uni.infogather.QueryProcessor;
import de.mannheim.uni.mongodb.CSV2MongoParallelWriter;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.pipelines.Pipeline.ExecutionModel;
import de.mannheim.uni.pipelines.Pipeline.RankingType;
import de.mannheim.uni.searchjoin.SearchJoin;
import de.mannheim.uni.statistics.ValuesDistributionAnalyzer;

/**
 * @author petar
 * 
 */
public class SearchJoinMain {
	public static enum FunctionType {
		index, searchJoin, stats, insertInMongo
	};

	public static void main(String[] args) {

		if (args == null || args.length < 3) {
			System.out.println("Please provide all arguments!");
			return;
		}
		// create the pipeline
		String functionNameArgs = args[0];
		String pipeName = args[1];
		String configFile = args[2];
		Pipeline pipeline = Pipeline.getPipelineFromConfigFile(pipeName,
				configFile);

		FunctionType functionName = FunctionType.valueOf(functionNameArgs);

		switch (functionName) {
		case index:
			Map<String, ReadTableType> repos = new HashMap<String, ReadTableType>();
			for (int i = 3; i < args.length; i += 3) {

				repos.put(args[i], ReadTableType.valueOf(args[i + 1]));
			}
			ParallelIndexer indexer = new ParallelIndexer(pipeline);
			// Indexer indexer = new Indexer(pipeline);
			indexer.indexRepos(repos);
			break;

		case searchJoin:
			if (args.length < 4) {
				System.out.println("Please provide query table!");
				return;
			}

			if(pipeline.getExecutionModel()==ExecutionModel.MSE)
			{
				SearchJoin searchJoin = new SearchJoin(pipeline);
	
				String queryTablePath = args[3];
				// set the additional header filter
				if (args.length >= 4) {
					for (int i = 4; i < args.length; i++) {
						pipeline.getHeaderRefineAttrs().add(args[i]);
						System.out.println("added headers: " + args[i]);
					}
				}
                                
				searchJoin.searchJoinForTable(queryTablePath);
	
				pipeline.getIndexManager().closeIndexReader();
			}
			else
			{
				QueryProcessor qp = new QueryProcessor(pipeline);
				
				qp.AugmentTable(args[3], args[4]);
			}

			break;

		case stats:
			ValuesDistributionAnalyzer dist = new ValuesDistributionAnalyzer();
			dist.makeStatsForRepo(args[3], pipeline,
					ReadTableType.valueOf(args[4]));
			break;
		case insertInMongo:
			CSV2MongoParallelWriter mongoWriter = new CSV2MongoParallelWriter(
					pipeline);
			mongoWriter.insertFilesInMongo(args[2],
					ReadTableType.valueOf(args[3]));

			break;

		default:
			// throw some exception
			break;
		}

	}
}
