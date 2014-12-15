package de.mannheim.uni.pipelines;

import de.mannheim.uni.pipelines.Pipeline.KeyIdentificationType;

public class PipelineFactory {
	/**
	 * @author petar
	 *
	 */
	public static enum PipelineType {
		simple, keyCompaundIdentification, keySignleIdentification, singleWithRefineAttrs
	};

	public static Pipeline getPipeline(PipelineType pipeType,
			String pipelineName, String indexLocation) {
		Pipeline pipeline = null;
		switch (pipeType) {

		case simple:
			pipeline = getSimplePipeline(pipelineName, indexLocation);
			break;

		case keySignleIdentification:
			pipeline = getKeyIdentificationPipeline(pipelineName,
					indexLocation, KeyIdentificationType.single);
			break;
		case keyCompaundIdentification:
			pipeline = getKeyIdentificationPipeline(pipelineName,
					indexLocation, KeyIdentificationType.compaund);
			break;
		case singleWithRefineAttrs:
			pipeline = getKeyIdentificationPipeline(pipelineName,
					indexLocation, KeyIdentificationType.singleWithRefineAttrs);
			break;

		default:
			// throw some exception
			break;
		}
		return pipeline;
	}

	private static Pipeline getKeyIdentificationPipeline(String pipelineName,
			String indexLocation, KeyIdentificationType keyType) {
		Pipeline pipeline = new Pipeline(pipelineName, indexLocation);
		pipeline.setKeyidentificationType(keyType);
		return pipeline;
	}

	private static Pipeline getSimplePipeline(String pipelineName,
			String indexLocation) {
		Pipeline pipeline = new Pipeline(pipelineName, indexLocation);
		return pipeline;
	}

}
