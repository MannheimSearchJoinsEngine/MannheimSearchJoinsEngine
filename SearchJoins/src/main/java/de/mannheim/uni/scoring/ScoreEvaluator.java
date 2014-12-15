package de.mannheim.uni.scoring;

import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.LabelBasedComparer;

public class ScoreEvaluator {

	private Pipeline pipeline;

	public static ScoreEvaluator get(Pipeline p) {
		// maybe create a specific evaluator based on the configuration ...

		return new ScoreEvaluator(p);
	}

	protected ScoreEvaluator(Pipeline p) {
		this.pipeline = p;
	}

	public double assessIndexEntry(IndexEntry e) {
		double score = e.getLuceneScore();

		for (IndexEntry r : e.getRefineAttrs())
			score += r.getLuceneScore() * pipeline.getRefineAttributesFactor();

		e.setTotalScore((float) score);

		return score;
	}

	public double assessJoinResultLuceneScore(JoinResult r) {
		double score = 0.0;
		int cnt = 0;

		for (IndexEntry e : r.getJoinPairs().values()) {
			score += assessIndexEntry(e);
			cnt++;
		}
		r.setLuceneTotalRank(score);

		return score;
	}

	public double assessJoinResult(JoinResult r, double maxScore, int maxJoins) {
		int count = r.getCount();
		double luceneAverageRank = r.getLuceneTotalRank() / (double) count;
		double rank = 0.0;

		switch (pipeline.getRankingType()) {
		case queryTableCoverage:
			rank = count * luceneAverageRank / r.getLeftColumnDistinctValues();
			break;

		case entityTableCoverage:
			rank = count * luceneAverageRank / r.getRightColumncardinality();
			break;
		case queryEntitySum:
			rank = count * luceneAverageRank / r.getLeftColumnDistinctValues()
					+ count * luceneAverageRank / r.getRightColumncardinality();
			rank = count * luceneAverageRank / r.getRightColumncardinality();
			break;
		case queryTableCoverageNormalized:
			/*
			 * // Calculate score for left side (query table) // a value of 1.0
			 * means all rows are matched, 0.0 means no rows are // matched
			 * double left = 1.0 - (double) (r.getLeftColumnCardinality() -
			 * count) / (double) r.getLeftColumnCardinality();
			 * 
			 * // Calculate score for right side (entity table) // Use the
			 * number of query rows if the entity table contains less // rows //
			 * Otherwise, very small tables (1 row) will be ranked too high // a
			 * value of 1.0 means all rows are matched and there are no //
			 * additional rows double m =
			 * Math.max(r.getRightColumncardinality(),
			 * r.getLeftColumnCardinality()); double right = 1.0 - (double) (m -
			 * count) / m;
			 * 
			 * // Calculate the final score by multiplying the avg. rank with
			 * the // scores for both tables rank = luceneAverageRank * left *
			 * right;
			 */

			int joinPairs = r.getCount();

			if (joinPairs < r.getLeftColumnCardinality() * 0.3)
				// a match with too few matched keys should not get a high
				// score, so increase the number of rows, by wich we divide the
				// whole score
				joinPairs = r.getLeftColumnCardinality();

			double myScore = r.getLuceneTotalRank() / (double) joinPairs;
			double bestScore = maxScore / (double) maxJoins;

			rank = myScore / bestScore;

			break;
		}

		r.setRank(rank);

		// rank modification by label based schema matching
		// first normalize score
		// double labelMin = -3.0 *
		// LabelBasedComparer.DBPEDIA_EXACT_MATCH_SCORE;
		// double labelMax = 3.0 * LabelBasedComparer.DBPEDIA_EXACT_MATCH_SCORE
		// + 1;
		double labelScore = getNormalizedLabelScore(r
				.getLabelBasedSchemaMatchingRank());
		// (r.getLabelBasedSchemaMatchingRank() - labelMin) / (labelMax -
		// labelMin);

		// combine scores
		double rankWeight = 0.5;
		double labelWeight = 0.5;
		double finalScore = rankWeight * rank + labelWeight * labelScore;

		return finalScore;
	}

	public double getNormalizedLabelScore(double score) {
		double labelMin = -3.0 * LabelBasedComparer.DBPEDIA_EXACT_MATCH_SCORE;
		double labelMax = 3.0 * LabelBasedComparer.DBPEDIA_EXACT_MATCH_SCORE + 1;

		return (score - labelMin) / (labelMax - labelMin);
	}

	public double getNormalizedLabelScoreForFusion(double score) {
		double max = LabelBasedComparer.WORDNET_EXACT_MATCH_SCORE_DUPLICATES;
		double min = 0;

		return (score - min) / (max - min);
	}

	/**
	 * Calculates a score that rewards complementary values the score is the sum
	 * of all (usual) score values plus an additional value for each
	 * complementary row, divided by the total number of rows
	 * 
	 * @param sv
	 * @return
	 */
	public double getComplementaryColumnSimilarity(ColumnScoreValue sv) {
		return (((double) sv.getComplementCount() * pipeline
				.getComplementaryScore()) + sv.getSum())
				/ (double) sv.getTotalCount();
	}

	public double getFinalColumnSimilarity(ColumnScoreValue instanceScore,
			ColumnScoreValue labelScore, ColumnScoreValue comInstaceScore) {
		// determine score threshold
		double lblThreshold = instanceScore.getType().equals(
				ColumnDataType.string.toString()) ? pipeline
				.getDuplicateLimitInstanecLabelString() : pipeline
				.getDuplicateLimitInstanecLabelNumeric();

		// get initial score value
		double score = 0;
		// if the label-based score is above some threshold, we use the
		// complementary score, which rewards complementary columns
		if (comInstaceScore != null) {
			score = comInstaceScore.getAverage();
			return score;
		}
		if (getNormalizedLabelScoreForFusion(labelScore.getAverage()) >= lblThreshold)
			score = getComplementaryColumnSimilarity(instanceScore);
		else
			score = instanceScore.getAverage();

		return score;
	}
}
