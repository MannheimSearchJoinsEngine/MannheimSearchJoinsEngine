package de.mannheim.uni.mse;

import java.util.ArrayList;
import java.util.Map;

import de.mannheim.uni.datafusion.DataFuser;
import de.mannheim.uni.index.TableInMemoryIndex;
import de.mannheim.uni.infogather.QueryProcessor;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.searchjoin.SearchJoin;

public class SearchJoinExecutor {

	Pipeline pipe;

	public SearchJoinExecutor()
	{
		this(new Pipeline("default", ""));
	}
	
	public SearchJoinExecutor(Pipeline pipeline)
	{
		pipe = pipeline;
	}
	
	public Table searchJoinMSE(String tablePath)
	{
		SearchJoin sj = new SearchJoin(pipe);
		
		Table t= sj.searchJoinForTable(tablePath);
		
		return t;
	}
	
	public Table searchJoinInfoGather(String tablePath, String attributeName)
	{
		QueryProcessor ig = new QueryProcessor(pipe);
		
		Table t = ig.AugmentTable(tablePath, attributeName);
		
		return t;
	}
	
	public Table joinAndMatchTables(Table table1, Table table2)
	{
		Join j = new Join(pipe);
		
		Map<String, JoinResult> joinResult = j.table2JoinResult(table1.getFirstKey(), table1, table2, null);
		
		DataFuser f = new DataFuser(pipe, new TableInMemoryIndex(table2));
		
		return f.fuseQueryTableWithEntityTables(table1, new ArrayList<JoinResult>(joinResult.values()), null);
	}
	
}
