package de.mannheim.uni.model;

import java.util.logging.Logger;

import de.mannheim.uni.pipelines.Pipeline;

public class SearchJoinsObject {

	private Pipeline pipeline;
	
	public Pipeline getPipeline() {
		return pipeline;
	}
	
	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}
	
	public SearchJoinsObject(Pipeline pipeline)
	{
		setPipeline(pipeline);
	}
	
	public SearchJoinsObject(SearchJoinsObject initialiser)
	{
		this(initialiser.getPipeline());
	}
	
	protected Logger getLogger()
	{
		return pipeline.getLogger();
	}
}
