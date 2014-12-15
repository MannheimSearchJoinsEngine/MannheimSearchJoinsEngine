package de.mannheim.uni.pipelines;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.commons.io.FileUtils;

import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.index.infogather.AttributesIndexManager;
import de.mannheim.uni.index.infogather.KeyIndexManager;

/**
 * @author petar
 * 
 */
public class Pipeline {

	private static final String CONF_FILE_PATH = "searchJoins.conf";

	public static enum KeyIdentificationType {
		none, single, compaund, singleWithRefineAttrs
	};

	public static enum RankingType {
		queryTableCoverage, entityTableCoverage, queryTableCoverageNormalized, queryEntitySum
	};

	public static enum IndexingType {
		wordTokens, Ngrams
	};

	public static enum NumericResoulution {
		median, average, provenance, voting
	}

	public static enum StringResoulution {
		longest, provenance, voting
	}

	public static enum ExecutionModel {
		MSE, INFOGATHER
	}

	private static Pipeline current;

	private String pipelineName;

	private KeyIdentificationType keyidentificationType;

	private double keyUniqueness;

	private double keyNullValuesFraction;

	private double averageKeyValuesLimitMax;

	private double averageKeyValuesLimitMin;

	private boolean removeNonStrings;

	private RankingType rankingType;

	private String indexLocation;

	private String headersIndexLocation;

	private int indexingMode;

	private boolean renewIndex;

	private IndexingType indexingType;

	private boolean useEditDistanceSearch;
	private int maxEditDistance = 1;

	private Logger logger;

	private Logger indexLogger;

	private int maxMatchedTables;

	private int instanceStartIndex;

	private int nmRetrievedDocsFromIndex;

	private boolean searchExactMatches;
	private boolean searchExactColumnHeaders;
	private boolean searchStemmedColumnHeaders;
	private boolean searchStemmedKeys;

	private String fastjoinMeasure;
	private double fastjoinDelta;
	private double fastjoinTau;
	private double fastjoinMinConf;
	private String fastjoinPath;

	private boolean useLabelBasedSchema;

	private boolean useInstanceBasedSchema;

	private double dataFusionColumnDensity;
	private double dataFusionRowDensity;

	private boolean reuseSearchResults;

	private double duplicateLimitInstanceString;
	private double duplicateLimitInstanceNumeric;

	private double duplicateLimitLabelString;

	private double duplicateLimitInstanecLabelString;
	private double duplicateLimitInstanecLabelNumeric;

	private double complementaryThreshold;
	private double complementaryScore;

	private double instanceMatchingSampleRatio = 1.0;

	private boolean skipCleaning = false;

	private boolean useExperimentalFusing;

	private boolean reuseColumnSimilarities;

	private boolean useCompleteComparer;

	private boolean isSilent = false;

	private String customStopWordsFile = null;

	// INFO GATHER PARAMETERS

	private String infoGatherKeyIndexLocation;
	private KeyIndexManager infoGatherKeyIndexManager;

	private String infoGatherAttributeIndexLocation;
	private AttributesIndexManager infoGatherAttributeIndexManager;
	private String t2pprIndexPath;

	private ExecutionModel executionModel;

	public int getIndexingMode() {
		return indexingMode;
	}

	public void setIndexingMode(int indexingMode) {
		this.indexingMode = indexingMode;
	}

	public ExecutionModel getExecutionModel() {
		return executionModel;
	}

	private String graphMappedIDFile;

	public String getGprahMappedIDFile() {
		return graphMappedIDFile;
	}

	public String getT2pprIndexPath() {
		return t2pprIndexPath;
	}

	public String getInfoGatherAttributeIndexLocation() {
		return infoGatherAttributeIndexLocation;
	}

	public AttributesIndexManager getInfoGatherAttributeIndexManager() {
		return infoGatherAttributeIndexManager;
	}

	public KeyIndexManager getInfoGatherKeyIndexManager() {
		return infoGatherKeyIndexManager;
	}

	public void setInfoGatherKeyIndexManager(
			KeyIndexManager infoGatherKeyIndexManager) {
		this.infoGatherKeyIndexManager = infoGatherKeyIndexManager;
	}

	public String getInfoGatherKeyIndexLocation() {
		return infoGatherKeyIndexLocation;
	}

	public void setInfoGatherKeyIndexLocation(String infoGatherKeyIndexLocation) {
		this.infoGatherKeyIndexLocation = infoGatherKeyIndexLocation;
	}

	public boolean isReuseColumnSimilarities() {
		return reuseColumnSimilarities;
	}

	public boolean isUseCompleteComparer() {
		return useCompleteComparer;
	}

	public void setReuseColumnSimilarities(boolean reuseColumnSimilarities) {
		this.reuseColumnSimilarities = reuseColumnSimilarities;
	}

	private NumericResoulution numericResolution;
	private StringResoulution stringResoulution;

	public NumericResoulution getNumericResolution() {
		return numericResolution;
	}

	public StringResoulution getStringResoulution() {
		return stringResoulution;
	}

	public List<String> getTrustYourFriendOrder() {
		return trustYourFriendOrder;
	}

	public void setNumericResolution(NumericResoulution numericResolution) {
		this.numericResolution = numericResolution;
	}

	public void setStringResoulution(StringResoulution stringResoulution) {
		this.stringResoulution = stringResoulution;
	}

	public boolean isUseExperimentalFusing() {
		return useExperimentalFusing;
	}

	public void setUseExperimentalFusing(boolean useExperimentalFusing) {
		this.useExperimentalFusing = useExperimentalFusing;
	}

	public String getHeadersIndexLocation() {
		return headersIndexLocation;
	}

	public void setHeadersIndexLocation(String headersIndexLocation) {
		this.headersIndexLocation = headersIndexLocation;
	}

	public double getComplementaryScore() {
		return complementaryScore;
	}

	public void setComplementaryScore(double complementaryScore) {
		this.complementaryScore = complementaryScore;
	}

	public double getComplementaryThreshold() {
		return complementaryThreshold;
	}

	public void setComplementaryThreshold(double complementaryThreshold) {
		this.complementaryThreshold = complementaryThreshold;
	}

	public de.mannheim.uni.index.AttributesIndexManager getAttributesIndexManager() {
		return attributesIndexManager;
	}

	public void setAttributesIndexManager(
			de.mannheim.uni.index.AttributesIndexManager attributesIndexManager) {
		this.attributesIndexManager = attributesIndexManager;
	}

	private List<String> trustYourFriendOrder;

	private IndexManager indexManager;

	private de.mannheim.uni.index.AttributesIndexManager attributesIndexManager;

	private int minRows;

	private int minCols;

	private double refineAttributesFactor;

	private List<String> headerRefineAttrs;

	/**
	 * all tables sources that are included in this list will be ignored for the
	 * search
	 */
	private List<String> tablesToAvoid;

	/**
	 * all tables with more then this will be ignored during indexSearch
	 */
	private int maxTableRowsNumber;

	// all files above this size will not be indexed
	private double maxFileSize;

	private String wordnetDictionaryPath;

	private String configFileLocation;

	/**
	 * Creates new pipeline object and initializes it from config file
	 * 
	 * @param name
	 */
	public static Pipeline getPipelineFromConfigFile(String name,
			String configFile) {
		Pipeline pip = new Pipeline("", "");
		/*
		 * Read configuration from file
		 */
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream(new File(configFile)));
		} catch (Exception e) {
			System.out.println("Error while trying to read importer.conf");
			e.printStackTrace();
		}
		pip.setConfigFileLocation(configFile);

		pip.pipelineName = name;
		pip.logger = createLogger(name);
		pip.indexLogger = createLogger(name + "Index");

		// key configs
		pip.keyidentificationType = KeyIdentificationType.valueOf(properties
				.getProperty("key.identification"));
		pip.keyUniqueness = Double.parseDouble(properties
				.getProperty("key.uniqueness"));
		pip.keyNullValuesFraction = Double.parseDouble(properties
				.getProperty("key.nullValuesFraction"));
		pip.removeNonStrings = Boolean.parseBoolean(properties
				.getProperty("key.removeNonStrings"));
		pip.averageKeyValuesLimitMin = Double.parseDouble(properties
				.getProperty("key.averageKeyValuesLimitMin"));
		pip.averageKeyValuesLimitMax = Double.parseDouble(properties
				.getProperty("key.averageKeyValuesLimitMax"));

		// ranking configs
		pip.rankingType = RankingType.valueOf(properties
				.getProperty("rankingType"));
		pip.refineAttributesFactor = Double.parseDouble(properties
				.getProperty("refineAttrsFacotr"));
		pip.headerRefineAttrs = new ArrayList<String>();

		// index configs
		pip.indexLocation = properties.getProperty("index.location");
		pip.headersIndexLocation = properties
				.getProperty("index.attributesLocation");
		pip.indexingMode = Integer.parseInt(properties
				.getProperty("index.mode"));
		pip.renewIndex = Boolean.parseBoolean(properties
				.getProperty("index.renewIndex"));
		pip.indexingType = IndexingType.valueOf(properties
				.getProperty("index.type"));
		pip.useEditDistanceSearch = Boolean.parseBoolean(properties
				.getProperty("index.useEditDistanceSearch"));
		pip.maxEditDistance = Integer.parseInt(properties
				.getProperty("index.maxEditDistance"));
		pip.instanceStartIndex = Integer.parseInt(properties
				.getProperty("index.instanceStartIndex"));
		pip.nmRetrievedDocsFromIndex = Integer.parseInt(properties
				.getProperty("index.nmRetrievedDocsFromIndex"));
		pip.maxFileSize = Double.parseDouble(properties
				.getProperty("index.maxFileSize"));
		pip.maxTableRowsNumber = Integer.parseInt(properties
				.getProperty("index.maxTableRowsNumber"));
		pip.searchExactMatches = Boolean.parseBoolean(properties
				.getProperty("index.searchExactMatches"));
		pip.searchExactColumnHeaders = Boolean.parseBoolean(properties
				.getProperty("index.searchExactColumnHeaders"));
		pip.tablesToAvoid = readTablesToAvoid(properties
				.getProperty("index.tablesToAvoid"));
		pip.tablesToAvoid = readTablesToAvoid(properties
				.getProperty("index.tablesToAvoid"));
		pip.minCols = Integer.parseInt(properties.getProperty("index.minCol"));
		pip.minRows = Integer.parseInt(properties.getProperty("index.minRow"));
		pip.customStopWordsFile = properties
				.getProperty("index.customSearchStopWordList");
		pip.searchStemmedColumnHeaders = Boolean.parseBoolean(properties
				.getProperty("index.searchStemmedColumnHeaders"));
		pip.searchStemmedKeys = Boolean.parseBoolean(properties
				.getProperty("index.searchStemmedKeys"));

		// fastjoin config
		pip.fastjoinMeasure = properties.getProperty("fastjoin.Measure");
		pip.fastjoinDelta = Double.parseDouble(properties
				.getProperty("fastjoin.Delta"));
		pip.fastjoinTau = Double.parseDouble(properties
				.getProperty("fastjoin.Tau"));
		pip.fastjoinMinConf = Double.parseDouble(properties
				.getProperty("fastjoin.MinConf"));
		pip.fastjoinPath = properties.getProperty("fastjoin.Path");

		// schema matching config
		pip.wordnetDictionaryPath = properties
				.getProperty("schema.wordNetPath");
		pip.useLabelBasedSchema = Boolean.parseBoolean(properties
				.getProperty("schema.useLabelBasedSchema"));
		pip.useInstanceBasedSchema = Boolean.parseBoolean(properties
				.getProperty("schema.useInstanceBasedSchema"));
		pip.instanceMatchingSampleRatio = Double.parseDouble(properties
				.getProperty("schema.instanceMatchingSampleRatio", "1.0"));

		// data fusion config
		pip.dataFusionColumnDensity = Double.parseDouble(properties
				.getProperty("data.columnDensity"));
		pip.dataFusionRowDensity = Double.parseDouble(properties
				.getProperty("data.rowDensity"));
		pip.maxMatchedTables = Integer.parseInt(properties
				.getProperty("data.maxMatchedTables"));

		// limits for duplicates
		pip.duplicateLimitInstanceNumeric = Double.parseDouble(properties
				.getProperty("data.duplicates.limit.instance.numeric"));
		pip.duplicateLimitInstanceString = Double.parseDouble(properties
				.getProperty("data.duplicates.limit.instance.string"));
		pip.duplicateLimitLabelString = Double.parseDouble(properties
				.getProperty("data.duplicates.limit.label.string"));
		pip.duplicateLimitInstanecLabelNumeric = Double.parseDouble(properties
				.getProperty("data.duplicates.limit.instancelabel.numeric"));
		pip.duplicateLimitInstanecLabelString = Double.parseDouble(properties
				.getProperty("data.duplicates.limit.instancelabel.string"));
		pip.trustYourFriendOrder = readTrustYourFriendOrder(properties
				.getProperty("data.sourcesOrder"));
		pip.complementaryThreshold = Double.parseDouble(properties
				.getProperty("data.duplicates.complementary.threshold"));
		pip.complementaryScore = Double.parseDouble(properties
				.getProperty("data.duplicates.complementary.score"));
		pip.stringResoulution = StringResoulution.valueOf(properties
				.getProperty("data.stringResolution"));
		pip.numericResolution = NumericResoulution.valueOf(properties
				.getProperty("data.numericResolution"));
		pip.useCompleteComparer = Boolean.parseBoolean(properties
				.getProperty("data.duplicates.useCompleteComparer"));

		// other config
		pip.reuseSearchResults = Boolean.parseBoolean(properties
				.getProperty("misc.reuseSearchResults"));
		pip.useExperimentalFusing = Boolean.parseBoolean(properties
				.getProperty("misc.useExperimentalFusing"));
		pip.reuseColumnSimilarities = Boolean.parseBoolean(properties
				.getProperty("misc.reuseColumnSimilarities"));
		pip.skipCleaning = Boolean.parseBoolean(properties
				.getProperty("misc.skipCleaning"));

		// infogather config
		pip.infoGatherKeyIndexLocation = properties
				.getProperty("infogather.keyIndexLocation");
		pip.infoGatherAttributeIndexLocation = properties
				.getProperty("infogather.attributeIndexLocation");
		pip.t2pprIndexPath = properties.getProperty("infogather.t2ppr");

		pip.graphMappedIDFile = properties
				.getProperty("infogather.graphMappedIDFile");

		pip.infoGatherKeyIndexManager = new KeyIndexManager(pip);
		pip.infoGatherAttributeIndexManager = new AttributesIndexManager(pip);

		// engine config
		pip.executionModel = ExecutionModel.valueOf(properties
				.getProperty("engine.executionmodel"));

		pip.indexManager = new IndexManager(pip);

		pip.attributesIndexManager = new de.mannheim.uni.index.AttributesIndexManager(
				pip);

		Pipeline.setCurrent(pip);

		return pip;
	}

	/**
	 * populates the list of tables to avoid from config file
	 * 
	 * @param props
	 * @return
	 */
	public static List<String> readTablesToAvoid(String props) {
		List<String> tablesToAvoid = new ArrayList<String>();
		if (props.length() == 1)
			return tablesToAvoid;
		String tables[] = props.split("\\|");
		for (String tab : tables)
			tablesToAvoid.add(tab);
		return tablesToAvoid;
	}

	/**
	 * populates the list of ordered sources from config file
	 * 
	 * @param props
	 * @return
	 */
	public static List<String> readTrustYourFriendOrder(String props) {
		List<String> tablesToAvoid = new ArrayList<String>();
		if (props.length() == 1)
			return tablesToAvoid;
		String tables[] = props.split("\\|");
		for (String tab : tables)
			tablesToAvoid.add(tab);
		return tablesToAvoid;
	}

	public Pipeline(String pipelineName, String indexLocation) {
		this.pipelineName = pipelineName;
		this.keyidentificationType = KeyIdentificationType.singleWithRefineAttrs;
		this.keyUniqueness = -10;
		this.keyNullValuesFraction = 0.02;
		this.removeNonStrings = false;
		this.rankingType = RankingType.queryTableCoverage;
		this.indexLocation = indexLocation;
		this.renewIndex = false;
		this.indexingType = IndexingType.wordTokens;
		this.useEditDistanceSearch = false;
		this.logger = createLogger(pipelineName);
		this.indexLogger = createLogger(pipelineName + "Index");
		this.instanceStartIndex = 2;
		this.averageKeyValuesLimitMin = 0;
		this.averageKeyValuesLimitMax = 1500;
		this.nmRetrievedDocsFromIndex = 1000;
		this.maxFileSize = 115; // MB
		this.maxTableRowsNumber = 0;
		this.searchExactMatches = false;
		this.tablesToAvoid = new ArrayList<String>();
		this.trustYourFriendOrder = new LinkedList<String>();
		this.minCols = 3;
		this.minRows = 5;
		this.headerRefineAttrs = new ArrayList<String>();
		this.stringResoulution = StringResoulution.voting;
		this.numericResolution = NumericResoulution.median;
		this.executionModel = ExecutionModel.MSE;
		this.dataFusionRowDensity = 1.0;
		// TODO Auto-generated constructor stub

		Pipeline.setCurrent(this);
	}

	public List<String> getHeaderRefineAttrs() {
		return headerRefineAttrs;
	}

	public void setHeaderRefineAttrs(List<String> headerRefineAttrs) {
		this.headerRefineAttrs = headerRefineAttrs;
	}

	public double getRefineAttributesFactor() {
		return refineAttributesFactor;
	}

	public void setRefineAttributesFactor(double refineAttributesFactor) {
		this.refineAttributesFactor = refineAttributesFactor;
	}

	public int getMinCols() {
		return minCols;
	}

	public int getMinRows() {
		return minRows;
	}

	public void setMinCols(int minCols) {
		this.minCols = minCols;
	}

	public void setMinRows(int minRows) {
		this.minRows = minRows;
	}

	public IndexManager getIndexManager() {
		return indexManager;
	}

	public Logger getIndexLogger() {
		return indexLogger;
	}

	public double getDuplicateLimitInstanceNumeric() {
		return duplicateLimitInstanceNumeric;
	}

	public void setDuplicateLimitInstanceNumeric(
			double duplicateLimitInstanceNumeric) {
		this.duplicateLimitInstanceNumeric = duplicateLimitInstanceNumeric;
	}

	public double getDuplicateLimitInstanceString() {
		return duplicateLimitInstanceString;
	}

	public void setDuplicateLimitInstanceString(
			double duplicateLimitInstanceString) {
		this.duplicateLimitInstanceString = duplicateLimitInstanceString;
	}

	public double getDuplicateLimitInstanecLabelNumeric() {
		return duplicateLimitInstanecLabelNumeric;
	}

	public void setDuplicateLimitInstanecLabelNumeric(
			double duplicateLimitInstanecLabelNumeric) {
		this.duplicateLimitInstanecLabelNumeric = duplicateLimitInstanecLabelNumeric;
	}

	public double getDuplicateLimitInstanecLabelString() {
		return duplicateLimitInstanecLabelString;
	}

	public void setDuplicateLimitInstanecLabelString(
			double duplicateLimitInstanecLabelString) {
		this.duplicateLimitInstanecLabelString = duplicateLimitInstanecLabelString;
	}

	public double getDuplicateLimitLabelString() {
		return duplicateLimitLabelString;
	}

	public void setDuplicateLimitLabelString(double duplicateLimitLabelString) {
		this.duplicateLimitLabelString = duplicateLimitLabelString;
	}

	public double getDataFusionColumnDensity() {
		return dataFusionColumnDensity;
	}

	public void setDataFusionColumnDensity(double dataFusionColumnDensity) {
		this.dataFusionColumnDensity = dataFusionColumnDensity;
	}

	public boolean isUseInstanceBasedSchema() {
		return useInstanceBasedSchema;
	}

	public void setUseInstanceBasedSchema(boolean useInstanceBasedSchema) {
		this.useInstanceBasedSchema = useInstanceBasedSchema;
	}

	public boolean isUseLabelBasedSchema() {
		return useLabelBasedSchema;
	}

	public void setUseLabelBasedSchema(boolean useLabelBasedSchema) {
		this.useLabelBasedSchema = useLabelBasedSchema;
	}

	public List<String> getTablesToAvoid() {
		return tablesToAvoid;
	}

	public void setTablesToAvoid(List<String> tablesToAvoid) {
		this.tablesToAvoid = tablesToAvoid;
	}

	public void setSearchExactMatches(boolean searchExactMatches) {
		this.searchExactMatches = searchExactMatches;
	}

	public boolean isSearchExactMatches() {
		return searchExactMatches;
	}

	public String getWordnetDictionaryPath() {
		return wordnetDictionaryPath;
	}

	public void setWordnetDictionaryPath(String wordnetDictionaryPath) {
		this.wordnetDictionaryPath = wordnetDictionaryPath;
	}

	public int getMaxTableRowsNumber() {
		return maxTableRowsNumber;
	}

	public void setMaxTableRowsNumber(int maxTableRowsNumber) {
		this.maxTableRowsNumber = maxTableRowsNumber;
	}

	public double getMaxFileSize() {
		return maxFileSize;
	}

	public void setMaxFileSize(double maxFileSize) {
		this.maxFileSize = maxFileSize;
	}

	public int getNmRetrievedDocsFromIndex() {
		return nmRetrievedDocsFromIndex;
	}

	public void setNmRetrievedDocsFromIndex(int nmRetrievedDocsFromIndex) {
		this.nmRetrievedDocsFromIndex = nmRetrievedDocsFromIndex;
	}

	public double getAverageKeyValuesLimitMax() {
		return averageKeyValuesLimitMax;
	}

	public double getAverageKeyValuesLimitMin() {
		return averageKeyValuesLimitMin;
	}

	public void setAverageKeyValuesLimitMax(double averageKeyValuesLimitMax) {
		this.averageKeyValuesLimitMax = averageKeyValuesLimitMax;
	}

	public void setAverageKeyValuesLimitMin(double averageKeyValuesLimitMin) {
		this.averageKeyValuesLimitMin = averageKeyValuesLimitMin;
	}

	public int getInstanceStartIndex() {
		return instanceStartIndex;
	}

	public void setInstanceStartIndex(int instanceStartIndex) {
		this.instanceStartIndex = instanceStartIndex;
	}

	private static Logger createLogger(String name) {
		Logger logger = Logger.getLogger(name + "Logger");
		FileHandler fh;

		try {

			// This block configure the logger with handler and formatter
			fh = new FileHandler(name + "Logger.log");
			logger.addHandler(fh);
			// logger.setLevel(Level.ALL);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

		} catch (SecurityException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return logger;
	}

	public Logger getLogger() {
		return logger;
	}

	public boolean isRemoveNonStrings() {
		return removeNonStrings;
	}

	public void setRemoveNonStrings(boolean removeNonStrings) {
		this.removeNonStrings = removeNonStrings;
	}

	public String getPipelineName() {
		return pipelineName;
	}

	public void setPipelineName(String pipelineName) {
		this.pipelineName = pipelineName;
	}

	public KeyIdentificationType getKeyidentificationType() {
		return keyidentificationType;
	}

	public void setKeyidentificationType(
			KeyIdentificationType keyidentificationType) {
		this.keyidentificationType = keyidentificationType;
	}

	public double getKeyUniqueness() {
		return keyUniqueness;
	}

	public void setKeyUniqueness(double keyUniqueness) {
		this.keyUniqueness = keyUniqueness;
	}

	public double getKeyNullValuesFraction() {
		return keyNullValuesFraction;
	}

	public void setKeyNullValuesFraction(double keyNullValuesFraction) {
		this.keyNullValuesFraction = keyNullValuesFraction;
	}

	public RankingType getRankingType() {
		return rankingType;
	}

	public void setRankingType(RankingType rankingType) {
		this.rankingType = rankingType;
	}

	public String getIndexLocation() {
		return indexLocation;
	}

	public void setIndexLocation(String indexLocation) {
		this.indexLocation = indexLocation;
	}

	public boolean isRenewIndex() {
		return renewIndex;
	}

	public void setRenewIndex(boolean renewIndex) {
		this.renewIndex = renewIndex;
	}

	public IndexingType getIndexingType() {
		return indexingType;
	}

	public void setIndexingType(IndexingType indexingType) {
		this.indexingType = indexingType;
	}

	public boolean isUseEditDistanceSearch() {
		return useEditDistanceSearch;
	}

	public void setUseEditDistanceSearch(boolean useEditDistanceSearch) {
		this.useEditDistanceSearch = useEditDistanceSearch;
	}

	public String getConfigFileLocation() {
		return configFileLocation;
	}

	public void setConfigFileLocation(String configFileLocation) {
		this.configFileLocation = configFileLocation;
	}

	public int getMaxMatchedTables() {
		return maxMatchedTables;
	}

	public void setMaxMatchedTables(int maxMatchedTables) {
		this.maxMatchedTables = maxMatchedTables;
	}

	public boolean isReuseSearchResults() {
		return reuseSearchResults;
	}

	public void setReuseSearchResults(boolean reuseSearchResults) {
		this.reuseSearchResults = reuseSearchResults;
	}

	public double getDataFusionRowDensity() {
		return dataFusionRowDensity;
	}

	public void setDataFusionRowDensity(double dataFusionRowDensity) {
		this.dataFusionRowDensity = dataFusionRowDensity;
	}

	public boolean isSilent() {
		return isSilent;
	}

	public void setSilent(boolean isSilent) {
		this.isSilent = isSilent;
	}

	public void setCustomStopWordsFile(String customStopWordsFile) {
		this.customStopWordsFile = customStopWordsFile;
	}

	public String getCustomStopWordsFile() {
		return customStopWordsFile;
	}

	private List<String> customStopWords;

	public List<String> getCustomStopWords() {
		if (customStopWords == null) {
			if (new File(customStopWordsFile).exists()) {
				try {
					customStopWords = FileUtils.readLines(new File(
							customStopWordsFile));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			} else
				customStopWords = new ArrayList<String>(0);

			return customStopWords;
		} else
			return customStopWords;
	}

	public static Pipeline getCurrent() {
		return current;
	}

	public static void setCurrent(Pipeline current) {
		Pipeline.current = current;
	}

	public int getMaxEditDistance() {
		return maxEditDistance;
	}

	public void setMaxEditDistance(int maxEditDistance) {
		this.maxEditDistance = maxEditDistance;
	}

	public boolean isSearchStemmedColumnHeaders() {
		return searchStemmedColumnHeaders;
	}

	public void setSearchStemmedColumnHeaders(boolean searchStemmedColumnHeaders) {
		this.searchStemmedColumnHeaders = searchStemmedColumnHeaders;
	}

	public boolean isSearchStemmedKeys() {
		return searchStemmedKeys;
	}

	public void setSearchStemmedKeys(boolean searchStemmedKeys) {
		this.searchStemmedKeys = searchStemmedKeys;
	}

	public String getFastjoinMeasure() {
		return fastjoinMeasure;
	}

	public void setFastjoinMeasure(String fastjoinMeasure) {
		this.fastjoinMeasure = fastjoinMeasure;
	}

	public double getFastjoinDelta() {
		return fastjoinDelta;
	}

	public void setFastjoinDelta(double fastjoinDelta) {
		this.fastjoinDelta = fastjoinDelta;
	}

	public double getFastjoinTau() {
		return fastjoinTau;
	}

	public void setFastjoinTau(double fastjoinTau) {
		this.fastjoinTau = fastjoinTau;
	}

	public double getFastjoinMinConf() {
		return fastjoinMinConf;
	}

	public void setFastjoinMinConf(double fastjoinMinConf) {
		this.fastjoinMinConf = fastjoinMinConf;
	}

	public String getFastjoinPath() {
		return fastjoinPath;
	}

	public void setFastjoinPath(String fastjoinPath) {
		this.fastjoinPath = fastjoinPath;
	}

	private List<String> header_filter = null;
	private boolean hasHeaderFilter = true;

	public List<String> getHeaderFilter() {
		if (header_filter == null || !hasHeaderFilter) {
			if (getHeaderRefineAttrs().size() > 1) {
				File f = new File(getHeaderRefineAttrs().get(1));

				if (f.exists())
					try {
						header_filter = FileUtils.readLines(f);

						// make the list distinct
						Set<String> distinct = new HashSet<String>(
								header_filter);
						header_filter = new ArrayList<String>(distinct);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				else {
					header_filter = new ArrayList<String>();
					header_filter.add(getHeaderRefineAttrs().get(1));
				}
			} else
				hasHeaderFilter = false;
		}
		return header_filter;
	}

	public boolean isSearchExactColumnHeaders() {
		return searchExactColumnHeaders;
	}

	public void setSearchExactColumnHeaders(boolean searchExactColumnHeaders) {
		this.searchExactColumnHeaders = searchExactColumnHeaders;
	}

	public boolean isSkipCleaning() {
		return skipCleaning;
	}

	public void setSkipCleaning(boolean skipCleaning) {
		this.skipCleaning = skipCleaning;
	}

	public double getInstanceMatchingSampleRatio() {
		return instanceMatchingSampleRatio;
	}

	public void setInstanceMatchingSampleRatio(
			double instanceMatchingSampleRatio) {
		this.instanceMatchingSampleRatio = instanceMatchingSampleRatio;
	}
}
