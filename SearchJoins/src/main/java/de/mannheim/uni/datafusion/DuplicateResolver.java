package de.mannheim.uni.datafusion;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import de.mannheim.uni.model.schema.ColumnObjectsMatch;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.parsers.DateUtil;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.utils.PipelineConfig;
import java.util.HashSet;
import java.util.Set;

public class DuplicateResolver {

    private Pipeline pipeline;

    public DuplicateResolver(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Table resolveDuplicates(Table table,
            TableObjectsMatch instanceMatches, TableObjectsMatch labelMatches) {
        Timer t = new Timer("Resolve duplicates");
        ArrayList<DuplicateResolverThread> duplicateResolvers = new ArrayList<DuplicateResolverThread>();
        // generate all pairs for label matching
        labelMatches.producePairs();
        ThreadPoolExecutor pool = new ThreadPoolExecutor(4, 8, 0,
                TimeUnit.SECONDS,
                new java.util.concurrent.ArrayBlockingQueue<Runnable>(Math.max(
                instanceMatches.getColumns().size(), 1)));

        List<TableColumn> columnsToBeChecked = new ArrayList<TableColumn>();

        for (ColumnObjectsMatch instanceMatch : instanceMatches.getColumns()) {
            columnsToBeChecked.add(instanceMatch.getColumn1());
            columnsToBeChecked.addAll(instanceMatch.getColumn2());
            DuplicateResolverThread tc = new DuplicateResolverThread(pipeline,
                    instanceMatch, labelMatches);
            duplicateResolvers.add(tc);
            pool.execute(tc);
        }
        pool.shutdown();
        try {
            pool.awaitTermination(1, TimeUnit.DAYS);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Table newTable = new Table();
        newTable.setFullPath(table.getFullPath());
        newTable.setHeader(table.getHeader());
        // newTable.setCompaundKeyColumns(table.getCompaundKeyColumns());
        for (DuplicateResolverThread cc : duplicateResolvers) {
            for (TableColumn tc : cc.getResults()) {
                if (!newTable.getColumns().contains(tc)) {
                    newTable.getColumns().add(tc);
                }
            }
        }
        // add columns that were not checked for merging at all
        for (TableColumn tc : table.getColumns()) {
            if (!columnsToBeChecked.contains(tc)
                    && !newTable.getColumns().contains(tc)) {
                newTable.getColumns().add(tc);
            }
        }
        t.stop();
        return newTable;
    }

    private class DuplicateResolverThread implements Runnable {

        private Pipeline pipeline;
        private ColumnObjectsMatch instanceMatch;
        private TableObjectsMatch labelMatch;
        private TableColumn leftColumn;
        private List<TableColumn> rigthColumns;
        private List<TableColumn> results;

        public List<TableColumn> getResults() {
            return results;
        }

        public DuplicateResolverThread(Pipeline pipeline,
                ColumnObjectsMatch instanceMatch, TableObjectsMatch labelMatch) {
            this.pipeline = pipeline;
            this.instanceMatch = instanceMatch;
            this.labelMatch = labelMatch;
            this.results = new ArrayList<TableColumn>();
            this.leftColumn = instanceMatch.getColumn1();
            this.rigthColumns = instanceMatch.getColumn2();
        }

        public void run() {
            resolveDuplicate();

        }

        public void resolveDuplicate() {
            switch (instanceMatch.getColumn1().getDataType()) {
                case string:
                case link:
                case bool:
                    if (instanceMatch.getScore() >= pipeline
                            .getDuplicateLimitInstanceString()) {
                        results.add(mergeColumns(leftColumn, rigthColumns));
                        break;
                    }
                    // check if there is a label match
                    if (labelMatch != null
                            && instanceMatch.getScore() >= pipeline
                            .getDuplicateLimitInstanecLabelString()) {
                        checkDuplicatesBasedOnLabelAndInstanceSim();

                    }

                    break;
                case numeric:
                case unit:
                case date:
                case coordinate:
                    if (instanceMatch.getScore() >= pipeline
                            .getDuplicateLimitInstanceNumeric()) {
                        results.add(mergeColumns(leftColumn, rigthColumns));
                        break;
                    }
                    if (labelMatch != null
                            && instanceMatch.getScore() >= pipeline
                            .getDuplicateLimitInstanecLabelNumeric()) {
                        checkDuplicatesBasedOnLabelAndInstanceSim();
                    }

                    break;
                default:
                    break;
            }
            if (results.size() == 0) {
                results.add(leftColumn);
                results.addAll(rigthColumns);
            }

        }

        /**
         * iterates over all label matched pairs and checks if they are also
         * instance matched, if yes then they will be merged
         *
         */
        private void checkDuplicatesBasedOnLabelAndInstanceSim() {
            List<TableColumn> allColumns = instanceMatch.getColumn2();
            allColumns.add(instanceMatch.getColumn1());
            // don't add columns that are already there to merge
            List<TableColumn> alreadyAdded = new ArrayList<TableColumn>();
            // add all columns that need to be merged
            Map<TableColumn, List<TableColumn>> columnsToMerge = new HashMap<TableColumn, List<TableColumn>>();
            for (int i = 0; i < allColumns.size(); i++) {
                TableColumn tb1 = allColumns.get(i);
                if (alreadyAdded.contains(tb1)) {
                    continue;
                }

                for (int j = 0; j < allColumns.size(); j++) {
                    TableColumn tb2 = allColumns.get(j);
                    if (tb1 != tb2) {
                        if (alreadyAdded.contains(tb2)) {
                            continue;
                        }

                        for (ColumnObjectsMatch mt : labelMatch
                                .getColumnPairs()) {
                            // check if they are label matched
                            if ((tb1 == mt.getColumn1()
                                    && tb2 == mt.getColumn2().get(0) || tb2 == mt
                                    .getColumn1()
                                    && tb1 == mt.getColumn2().get(0))
                                    && mt.getScore() >= pipeline
                                    .getDuplicateLimitLabelString()) {
                                alreadyAdded.add(tb1);
                                alreadyAdded.add(tb2);
                                List<TableColumn> tomerge = new ArrayList<TableColumn>();
                                tomerge.add(tb2);
                                if (columnsToMerge.containsKey(tb1)) {
                                    tomerge.addAll(columnsToMerge.get(tb1));
                                }
                                columnsToMerge.put(tb1, tomerge);
                                break;
                            }
                        }
                    }
                }
            }
            for (Entry<TableColumn, List<TableColumn>> entry : columnsToMerge
                    .entrySet()) {
                results.add(mergeColumns(entry.getKey(), entry.getValue()));
            }

        }

        /**
         * Merges two columns
         *
         * @return
         */
        private TableColumn mergeColumns(TableColumn leftColumn,
                List<TableColumn> rigthColumns) {
            TableColumn column = new TableColumn();
            String newHeader = leftColumn.getHeader()
                    + getHeaderFromList(rigthColumns, 0);
            column.setHeader(newHeader);
            String newDataSource = leftColumn.getDataSource()
                    + getHeaderFromList(rigthColumns, 1);
            column.setDataSource(newDataSource);
            column.setCleaningInfo(leftColumn.getCleaningInfo()
                    + getHeaderFromList(rigthColumns, 2));
            column.setDataType(leftColumn.getDataType());
            // check if some of the columns are key

            if (leftColumn.isKey() || checkListFroKeys(rigthColumns)) {
                column.setKey(true);
            }

            if (leftColumn.getDataType() == ColumnDataType.unit) {
                column.setBaseUnit(leftColumn.getBaseUnit());
            }
            
            // determine number of rows
            Set<Integer> rows = new HashSet<Integer>();
            rows.addAll(leftColumn.getValues().keySet());
            for(TableColumn c : rigthColumns)
                rows.addAll(c.getValues().keySet());
            
            // if this is exactly the same column
			/*
             * if (instanceMatch.getScore() == 1.0) {
             * column.setValues(leftColumn.getValues()); return column; }
             */
            StringBuilder sb = new StringBuilder();
            //for (Entry<Integer, String> entry : leftColumn.getValues().entrySet()) {
            //for(int index=pipeline.getInstanceStartIndex(); index<=numRows+pipeline.getInstanceStartIndex();index++) {
            for(int index : rows) {
                //String leftValue = entry.getValue();
                String leftValue=PipelineConfig.NULL_VALUE;
                if(leftColumn.getValues().containsKey(index))
                    leftValue = leftColumn.getValues().get(index);
                //int index = entry.getKey();
                List<String> rightValues = getValuesFromList(rigthColumns,
                        index);
                
                if (leftColumn.getHeader().contains("industry")) {
                    //pipeline.getLogger().info("table: " + leftColumn.getHeader());
                    sb.append("table: " + leftColumn.getHeader() + "\n");
                    sb.append("left: " + leftValue + "\n");
                    //pipeline.getLogger().info("left: " + leftValue);
                    for (String s : rightValues) {
                        //pipeline.getLogger().info("right: " + s);
                        sb.append("right: " + s + "\n");
                    }
                }
                // if the values are the same, put the first one in the final
                // column
                if (checkIfAllvaluesAreSame(rightValues, leftValue)) {
                    column.getValues().put(index, leftValue);
                    continue;
                }

                // **TAKE THE INFORMATION
                // pick the not null value
                if (leftValue.equalsIgnoreCase(PipelineConfig.NULL_VALUE) || leftValue.isEmpty()) {
                    column.getValues().put(index, getNonNullValue(rightValues));
                    continue;
                }
                String nonNull = getNonNullValue(rightValues);
                if (nonNull == null || nonNull.equalsIgnoreCase(
                        PipelineConfig.NULL_VALUE)) {
                    column.getValues().put(index, leftValue);
                    continue;
                }
                String newValue = leftValue;
                switch (leftColumn.getDataType()) {
                    case string:
                        newValue = leftValue;
                        switch (pipeline.getStringResoulution()) {
                            case longest:
                                // choose the longer one
                                newValue = getLargestValue(rightValues, leftValue);
                                break;
                            case voting:
                                newValue = votForFinalValue(rightValues, leftValue);
                                break;
                            default:
                                break;
                        }

                        break;
                    case bool:
                        // TODO resolve this
                        newValue = leftValue;
                        break;
                    case numeric:
                    case coordinate:
                    case unit:
                        // TODO resolve for coordinate
                        try {

                            switch (pipeline.getNumericResolution()) {
                                case median:
                                    newValue = getMedianValue(leftValue, rightValues);
                                    break;
                                case average:
                                    double val = getAverageFromList(leftValue,
                                            rightValues);
                                    newValue = Double.toString(val);
                                    break;
                                case voting:
                                    newValue = votForFinalValue(rightValues, leftValue);
                                    break;
                                default:
                                    break;
                            }

                        } catch (Exception e) {
                        }
                        break;

                    case date:
                        try {

                            long avgSeconds = getAverageSecondsFromDates(leftValue,
                                    rightValues);// (d1.getTime() / 1000L +
                            // d2.getTime() / 1000L) / 2;
                            Date averageDate = new Date(avgSeconds * 1000L);
                            newValue = averageDate.toString();
                        } catch (Exception e) {
                        }
                        break;
                    default:
                        break;
                }
                column.getValues().put(index, newValue);
            }
            pipeline.getLogger().info(
                    leftColumn.getHeader() + "(" + leftColumn.getDataSource()
                    + ") was merged with "
                    + getHeaderFromList(rigthColumns, 0) + "("
                    + getHeaderFromList(rigthColumns, 1) + ")" + "\n" + sb.toString());
            return column;
        }

        /**
         * votes the final value
         *
         * @param rightValues
         * @param leftValue
         * @return
         */
        private String votForFinalValue(List<String> rightValues,
                String leftValue) {
            Map<String, Integer> values = new HashMap<String, Integer>();
            values.put(leftValue, 1);
            String maxVoteValue = leftValue;
            int max = 1;
            for (String rV : rightValues) {
                int curV = 1;
                if (values.containsKey(rV)) {
                    curV += values.get(rV);
                }
                values.put(rV, curV);
                if (curV > max) {
                    max = curV;
                    maxVoteValue = rV;
                }
            }
            return maxVoteValue;
        }

        private String getMedianValue(String leftValue, List<String> rightValues) {

            List<Double> values = new ArrayList<Double>();
            values.add(Double.parseDouble(leftValue));

            for (String rV : rightValues) {
                values.add(Double.parseDouble(rV));
            }
            Collections.sort(values);
            double value = (values.size() % 2 == 0) ? values
                    .get(values.size() / 2) : values.get(values.size() / 2 + 1);
            return Double.toString(value);
        }

        private long getAverageSecondsFromDates(String leftValue,
                List<String> rigthColumns2) {
            long avgSeconds = 0;
            try {

                Date d1 = DateUtil.parse(leftValue);
                avgSeconds = d1.getTime() / 1000L;
                for (String s : rigthColumns2) {
                    Date d2 = DateUtil.parse(s);
                    avgSeconds = d2.getTime() / 1000L;
                }
            } catch (ParseException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            return avgSeconds / (1 + rigthColumns2.size());
        }

        private double getAverageFromList(String leftValue,
                List<String> rightValues) {
            double score = Double.parseDouble(leftValue);
            for (String s : rightValues) {
                score += Double.parseDouble(s);
            }
            return score / (1 + rightValues.size());
        }

        private boolean checkIfAllvaluesAreSame(List<String> rightValues,
                String leftValue) {
            if (leftValue == null) {
                leftValue = PipelineConfig.NULL_VALUE;
            }

            for (String s : rightValues) {
                if (!leftValue.equals(s)) {
                    return false;
                }
            }
            return true;
        }

        private String getLargestValue(List<String> rightValues,
                String leftValue) {
            int maxLength = leftValue.length();
            String finalString = leftValue;
            for (String s : rightValues) {
                if (s.length() > maxLength) {
                    maxLength = s.length();
                    finalString = s;
                }
            }
            return finalString;
        }

        private String getNonNullValue(List<String> rightValues) {

            for (String s : rightValues) {
                if (!PipelineConfig.NULL_VALUE.equalsIgnoreCase(s) && s != null && !s.isEmpty()) {
                    return s;
                }
            }
            return PipelineConfig.NULL_VALUE;
        }

        private List<String> getValuesFromList(List<TableColumn> rigthColumns2,
                int index) {
            List<String> values = new LinkedList<String>();

            for (TableColumn c : rigthColumns2) {
                values.add(c.getValues().get(index));
            }
            return values;
        }

        private boolean checkListFroKeys(List<TableColumn> rigthColumns2) {
            for (TableColumn c : rigthColumns2) {
                if (c.isKey()) {
                    return true;
                }
            }
            return false;
        }

        private String getHeaderFromList(List<TableColumn> rigthColumns2,
                int option) {
            String newHeader = "";
            for (TableColumn c : rigthColumns2) {
                switch (option) {
                    case 0:
                        newHeader += "||" + c.getHeader();
                        break;
                    case 1:
                        newHeader += "||" + c.getDataSource();
                        break;
                    case 2:
                        newHeader += "||" + c.getCleaningInfo();
                        break;
                    default:
                        break;
                }
            }

            return newHeader;
        }
    }
}
