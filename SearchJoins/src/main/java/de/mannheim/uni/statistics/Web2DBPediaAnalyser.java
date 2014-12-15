package de.mannheim.uni.statistics;

import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.datafusion.TableDataCleaner;
import de.mannheim.uni.evaluation.DictionaryManager;
import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.TableColumn.ColumnDataType;
import static de.mannheim.uni.model.TableColumn.ColumnDataType.date;
import static de.mannheim.uni.model.TableColumn.ColumnDataType.numeric;
import static de.mannheim.uni.model.TableColumn.ColumnDataType.string;
import de.mannheim.uni.parsers.DateUtil;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.instance.InstanceBasedComparer;
import de.mannheim.uni.schemamatching.label.LabelBasedColumnComparer;
import de.mannheim.uni.schemamatching.label.LabelBasedComparer;
import de.mannheim.uni.utils.PipelineConfig;
import de.mannheim.uni.utils.ValueAggregator;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import org.joda.time.DateTime;
import org.joda.time.Days;

public class Web2DBPediaAnalyser {

    Map<String, String> abbs = new HashMap<String, String>();
    Map<String, String> refs = new HashMap<String, String>();
    Map<String, Collection<String>> dictotionaryIndustry = new HashMap<String, Collection<String>>();
    Map<String, Collection<String>> dictotionaryDirector = new HashMap<String, Collection<String>>();

    //analyze 
    public void analyze(HashMap<String, Table> tables, HashMap<String, TableColumn> dbpediaColumns, Pipeline pipeline, boolean augmented) {

        try {
            //read in all the files that are used for abbreviations etc. -> TODO: better implementation, e.g. pass a folder with all the files as parameter
            BufferedReader read = new BufferedReader(new FileReader(new File("abbs.txt")));
            String line = read.readLine();
            while (line != null) {
                abbs.put(line.split("\t")[0], line.split("\t")[1]);
                line = read.readLine();
            }
            read.close();

            read = new BufferedReader(new FileReader(new File("capitals.txt")));
            line = read.readLine();
            while (line != null) {
                refs.put(line.split("\t")[0], line.split("\t")[1]);
                line = read.readLine();
            }
            read.close();

            DictionaryManager dictLoader = new DictionaryManager(pipeline);
            dictotionaryIndustry = dictLoader.loadDictionary("industry.dic");
            dictotionaryDirector = dictLoader.loadDictionary("director.dic");

            //we differentiate whether we evaluate an augmented or a full table
            //augmented: compute all evaluation measures for each column
            //full: compute the majority vote and apply the evaluation measures for the resulting column
            if (augmented) {
                CSVWriter w = new CSVWriter(new FileWriter("augweb2dbpedia.csv"));
                String[] headers = new String[]{"property", "header", "precision", "#web values"};
                w.writeNext(headers);
                for (Entry<String, Table> e : tables.entrySet()) {
                    List<String[]> allValues = analyseWithoutMajority(e.getValue(), dbpediaColumns.get(e.getKey()), pipeline);
                    for (String[] oneValue : allValues) {
                        w.writeNext(oneValue);
                    }
                }
                w.flush();
                w.close();
            } else {

                CSVWriter w = new CSVWriter(new FileWriter("web2dbpedia.csv"));

                String[] headers = new String[]{
                    "property",
                    "existing values",
                    "web values",
                    "complementary values",
                    "number of keys",
                    "number of evaluated rows",
                    "any contains",
                    "number of TP",
                    //               "additional values",
                    //               "add. min",
                    //                "add. max",
                    //                "add. avg",
                    "coverage (ex. values/number of keys)",
                    //                "precision (majority vote, equals)",
                    //                "precision (majority vote, contains)",
                    "precision upper bound (any contains/evaluated)",
                    //                "precision (majority vote, similarity)",
                    //                "recall",
                    "precision (TP/evaluated)"};
                w.writeNext(headers);

                for (Entry<String, Table> e : tables.entrySet()) {
                    //TableDataCleaner clean = new TableDataCleaner(e.getValue(), pipeline);
                    //Table cleanedTable = clean.removeNullRows(e.getValue(), dbpediaColumns.get(e.getKey()));
                    //String[] values = analyseWithMajority(cleanedTable, dbpediaColumns.get(e.getKey()), pipeline);
                    String[] values = analyseWithMajority(e.getValue(), dbpediaColumns.get(e.getKey()), pipeline);
                    w.writeNext(values);
                }
                w.flush();
                w.close();
            }
        } catch (IOException e) {
        }
    }

    /**
     * Compute the majority of each row in the given tables.
     * First, the values in a row are clustered if they are similar enough
     * (different similarity functions for each datatype). Take the largest 
     * cluster in terms of the number of occurences and define the
     * most occuring value in this cluster as the majority.
     * 
     * @param t
     * @param pipe
     * @return A map assigning the majority value to each row.
     */
    public Map<Integer, String> determineMajority(Table t, Pipeline pipe) {        
        TableColumn key = t.getFirstKey();
        System.out.println("determine majority for " + key.getHeader());
        TableColumn.ColumnDataType dbPediadatatype = null;
        Map<Integer, String> rowNumberToMajorityValue = new HashMap();
        Map<String, Integer> valueToCounterMap;
        int columnCounter = 0;
        //iterate through the rows and in this loop thourgh the columns
        for (Integer row : key.getValues().keySet()) {
            //skip the first to rows since they only contain the header and the datatype
            if (row == 0 || row == 1) {
                continue;
            }
            valueToCounterMap = new HashMap<String, Integer>();
            String dbpediaHeader = "";
            Map<String, Integer> counterMap = new HashMap<String, Integer>();
            //per cluster, we determine a representative value to identify the cluster, all other values
            //in this cluster are mapped to this value
            Map<String, Set<String>> representativeValueToAllValues = new HashMap<String, Set<String>>();
            columnCounter = 0;            
            for (TableColumn c : t.getColumns()) {                
                columnCounter++;
                //dbpedia columns (key or label)
                if (c.getHeader().contains("dbpedia_")) {                    
                    if (c.getHeader().contains("rdf-schema")) {
                        continue;
                    } else {
                        dbPediadatatype = c.getDataType();
                        dbpediaHeader = c.getHeader();
                        continue;
                    }
                }
                if(c.isKey()) {
                    System.out.println("key: " + c.getValues().get(row));
                    Map<ColumnDataType, Integer> types = new HashMap<ColumnDataType, Integer>();
                    for(TableColumn d : t.getColumns()) {    
                        if(types.keySet().contains(d.getDataType())) {
                            int counter = types.get(d.getDataType());
                            types.put(d.getDataType(), counter++);
                        }
                        else {
                            types.put(d.getDataType(), 1);
                        }
                    }
//                    int maxi =0;
//                    ColumnDataType mostType = ColumnDataType.numeric;
//                    for(ColumnDataType type : types.keySet()) {
//                        if(types.get(type) > maxi) {
//                            maxi = types.get(type);
//                            mostType = type;
//                        }
//                    }
                    //TODO: CHANGE!!!
                    //dbPediadatatype = mostType;                    
                    dbPediadatatype = ColumnDataType.numeric;
                    System.out.println(types);
                    continue;
                }
                
                dbPediadatatype = ColumnDataType.numeric;
                String value = c.getValues().get(row);
                if (isEmpty(c, row) || value == null || value.isEmpty()) {
                    continue;
                }
                try {
                   //try to avoid problems when the datatype is string but the
                   //row contains numbers, exculde these ones
                    Double.parseDouble(value);
                    if (dbPediadatatype == TableColumn.ColumnDataType.string) {
                        continue;
                    }                    
                } catch (Exception e) {
                }
                
                //count the occurences for each individual value,
                //later used to determine the majority value in the
                //majority cluster
                if (valueToCounterMap.containsKey(value)) {
                    int stringCounter = valueToCounterMap.get(value);
                    stringCounter++;
                    valueToCounterMap.put(value, stringCounter);
                } else {
                    valueToCounterMap.put(value, 1);
                }
                
                //the counter map is used to count the clusters 
                //the cluster with the highest counter is chosen as the majority
                if (counterMap.isEmpty()) {
                    counterMap.put(value, 1);
                    Set<String> firstValue = new HashSet<String>();
                    firstValue.add(value);
                    representativeValueToAllValues.put(value, firstValue);
                    continue;
                }

                boolean foundGroup = false;
                //iterate through all existing clusterand check whether the current
                //value is similar enough to one cluster
                for (String keyValue : representativeValueToAllValues.keySet()) {
                    switch (dbPediadatatype) {                        
                        case numeric:
                            try {
                                Double doubleValue = Double.parseDouble(value.replaceAll("[^0-9\\.\\,\\-]", ""));
                                //go thorugh all the values in this cluster 
                                for (String alternativeValues : representativeValueToAllValues.get(keyValue)) {
                                    double doubleKeyValue = Double.parseDouble(alternativeValues.replaceAll("[^0-9\\.\\,\\-]", ""));
                                    double score;
                                    if (doubleValue == doubleKeyValue) {
                                        score = 1.0;
                                    } else {
                                        score = 0.5 * Math.min(Math.abs(doubleKeyValue), Math.abs(doubleValue)) / Math.max(Math.abs(doubleKeyValue), Math.abs(doubleValue));
                                        //transformation from miles in kilometers or the other way around for area and density
                                        if (dbpediaHeader.contains("area") || dbpediaHeader.contains("density")) {
                                            double convertedScore = 0.5 * Math.min(Math.abs(doubleKeyValue * 0.386102159), Math.abs(doubleValue)) / Math.max(Math.abs(doubleKeyValue * 0.386102159), Math.abs(doubleValue));
                                            if (convertedScore > score) {
                                                score = convertedScore;
                                            } else {
                                                convertedScore = 0.5 * Math.min(Math.abs(doubleKeyValue), Math.abs(doubleValue * 0.386102159)) / Math.max(Math.abs(doubleKeyValue), Math.abs(doubleValue * 0.386102159));
                                                if (convertedScore > score) {
                                                    score = convertedScore;
                                                }
                                            }
                                        }
                                        //founding year is not recognized as date
                                        if (dbpediaHeader.contains("year")) {
                                            if (counterMap.containsKey(keyValue) && (doubleValue == doubleKeyValue
                                                    || doubleValue == doubleKeyValue + 1 || doubleValue + 1 == doubleKeyValue)) {
                                                int currentCounter = counterMap.get(keyValue);
                                                currentCounter++;
                                                counterMap.put(keyValue, currentCounter);
                                                representativeValueToAllValues.get(keyValue).add(value);
                                                foundGroup = true;
                                                break;
                                            }
                                        }

                                        //scaling (does not really work quite good)
//                                        double scaledScore = 0.5 * Math.min(Math.abs(doubleKeyValue * 1000), Math.abs(doubleValue)) / Math.max(Math.abs(doubleKeyValue * 1000), Math.abs(doubleValue));
//                                        if (scaledScore > score) {
//                                            score = scaledScore;
//                                        } else {
//                                            scaledScore = 0.5 * Math.min(Math.abs(doubleKeyValue), Math.abs(doubleValue * 1000)) / Math.max(Math.abs(doubleKeyValue), Math.abs(doubleValue * 1000));
//                                            if (scaledScore > score) {
//                                                score = scaledScore;
//                                            }
//                                        }
                                    }
                                    //if the score is above the threshold, put
                                    //this value into the cluster
                                    if (score > 0.45 && counterMap.containsKey(keyValue)) {
                                        int currentCounter = counterMap.get(keyValue);
                                        currentCounter++;
                                        counterMap.put(keyValue, currentCounter);
                                        representativeValueToAllValues.get(keyValue).add(value);
                                        foundGroup = true;
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                            }
                            break;
                        case date:
                            try {
                                for (String alternativeValues : representativeValueToAllValues.get(keyValue)) {
                                    Date d1 = DateUtil.parse(value);
                                    Date d2 = DateUtil.parse(alternativeValues);

                                    int dateDiff = Math.abs(Days.daysBetween(new DateTime(d1), new DateTime(d2)).getDays());

                                    //if more than 360 days apart, but
                                    //the value in the current cluster
                                    if (dateDiff > 360 && counterMap.containsKey(keyValue)) {
                                        int currentCounter = counterMap.get(keyValue);
                                        currentCounter++;
                                        counterMap.put(keyValue, currentCounter);
                                        representativeValueToAllValues.get(keyValue).add(value);
                                        foundGroup = true;
                                        break;
                                    }
                                }
                            } catch (Exception e) {
                            }
                            break;
                        case string:
                            LabelBasedComparer lb = new LabelBasedColumnComparer(pipe, t);
                            for (String alternativeValues : representativeValueToAllValues.get(keyValue)) {
                                if (counterMap.containsKey(keyValue) && (lb.matchStrings(alternativeValues, value) > 0.7 || alternativeValues.toLowerCase().contains(value.toLowerCase())
                                        || value.toLowerCase().contains(alternativeValues.toLowerCase()) || (abbs.containsKey(alternativeValues.toLowerCase()) && abbs.get(alternativeValues.toLowerCase()).equals(value.toLowerCase()))
                                        || (abbs.containsKey(value.toLowerCase()) && abbs.get(value.toLowerCase()).equals(alternativeValues.toLowerCase())))) {
                                    int currentCounter = counterMap.get(keyValue);
                                    currentCounter++;
                                    counterMap.put(keyValue, currentCounter);
                                    representativeValueToAllValues.get(keyValue).add(value);
                                    foundGroup = true;
                                    break;
                                }
                            }
                    }

                }
                //if none of the clusters fit, generate a new one
                if (!foundGroup) {
                    counterMap.put(value, 1);
                    Set<String> firstValue = new HashSet<String>();
                    firstValue.add(value);
                    representativeValueToAllValues.put(value, firstValue);
                }
            }

            //determine the majority cluster
            int maxOccurence = -1;
            String majorityValue = "";
            for (String s : counterMap.keySet()) {
                if (counterMap.get(s) > maxOccurence) {
                    maxOccurence = counterMap.get(s);
                    majorityValue = s;
                }
            }
            
            //determine the majority value in the majority cluster
            String internalMajority = "";
            if (!representativeValueToAllValues.isEmpty()) {
                maxOccurence = -1;
                for (String st : representativeValueToAllValues.get(majorityValue)) {
                    if (valueToCounterMap.get(st) > maxOccurence) {
                        maxOccurence = valueToCounterMap.get(st);
                        internalMajority = st;
                    }
                }
                rowNumberToMajorityValue.put(row, internalMajority);
            }
            for(String s : counterMap.keySet()) {
                System.out.println(s + " # " + counterMap.get(s));
            }  
        }        
        
        System.out.println("# columns: " + columnCounter);
        return rowNumberToMajorityValue;
    }

    /**
     * For an augmented table, the majority does not need to be determined.
     * Only the measures are computed and returned.
     * 
     * @param t
     * @param targetColumn
     * @param pipe
     * @return
     * @throws IOException 
     */
    public List<String[]> analyseWithoutMajority(Table t, TableColumn targetColumn, Pipeline pipe) throws IOException {
        TableColumn key = t.getFirstKey();
        TableColumn.ColumnDataType dbPediadatatype = targetColumn.getDataType();
        List<String> lst;
        List<String[]> all = new LinkedList<String[]>();
        String header = "";

        //iterate through the columns in the augmented table
        for (TableColumn c : t.getColumns()) {
            lst = new LinkedList<String>();
            System.out.println(c.getHeader());
            double evaluated = 0, correct = 0, counter = 0;
            if (c == targetColumn || c.isKey()) {
                if (c == targetColumn) {
                    header = c.getHeader();
                }
                continue;
            }
            for (Integer row : key.getValues().keySet()) {
                if (!isEmpty(c, row)) {
                    //dbpedia value null
                    if (isEmpty(targetColumn, row)) {
                        continue;
                    } else {
                        counter++;
                    }
                    String dbpediaValue = targetColumn.getValues().get(row);
                    String webValue = c.getValues().get(row);
                    //dbpedia value is null     
                    evaluated++;
                    //check whether the value in the column is a match
                    //with the DBpedia value
                    if (isMatch(dbPediadatatype, header, t, key, row, pipe, webValue, dbpediaValue)) {
                        correct++;
                    }
                }
            }
            double precision, coverage = 0;
            if (evaluated == 0) {
                precision = 0.0;
            } else {
                precision = correct / evaluated;
                coverage = counter;
            }
            lst.add(key.getHeader());
            lst.add(c.getHeader());
            lst.add(Double.toString(precision));
            lst.add(Double.toString(coverage));
            all.add(lst.toArray(new String[lst.size()]));
        }
        return all;
    }

    /**
     * 
     * 
     * @param t
     * @param targetColumn
     * @param pipe
     * @return
     * @throws IOException 
     */
    public String[] analyseWithMajority(Table t, TableColumn targetColumn, Pipeline pipe) {
        
        TableColumn key = t.getFirstKey();
        TableColumn.ColumnDataType dbPediadatatype = targetColumn.getDataType();

        System.out.println("Table " + t.getHeader());
        System.out.println("Columns: " + t.getColumns().size());
        System.out.println("Reference column: " + targetColumn.getHeader());

        int numExistingValues = 0; // there is a value from dbpedia
        int numRowsWithValue = 0; // there is a value from the webtables
        int evaluatedRows = 0;
        int numComplementaryValues = 0; // there is no value from dbpedia but from the webtables 
        int numAdditionalValues = 0;
        int numTotalValues = t.getFirstKey().getValues().size();
        int numTruePositives = 0;
        int numTruePositivesContains = 0;
        int numAnyContains = 0; // number for keys for which at least one value is contained in the DBPedia value
        int numTruePositiveSimilar = 0;
        int numDBpediaValues = 0;
        String dbepdiaHeader = targetColumn.getHeader();
        int numTruePositivesContainsSimilar = 0;

        ValueAggregator additionalValues = new ValueAggregator(); // number of additional values (there is one in dbpedia) for each instance
        List<String> lst = new LinkedList<String>();
        Map<Integer, String> rowNumbersToMajority = determineMajority(t, pipe);

        try {
        boolean written = false;
        File TP = new File("TP.txt");
        File FP = new File("FP.txt");
        BufferedWriter writeTP = new BufferedWriter(new FileWriter(TP, true));
        BufferedWriter writeFP = new BufferedWriter(new FileWriter(FP, true));

        //the resultMajority file contains the majority decision for each key
        File f = new File("resultMajority.csv");
        BufferedWriter write = new BufferedWriter(new FileWriter(f, true));
        write.append(t.getHeader() + "\n");
        for (int i : rowNumbersToMajority.keySet()) {
            write.append(i + "\t" + targetColumn.getValues().get(i) + "\t" + t.getFirstKey().getValues().get(i) +"\t" + rowNumbersToMajority.get(i) + "\n");
        }
        write.flush();
        write.close();

        //       System.out.println("size: " + rowNumbersToMajority.size() + " rows: " + key.getValues().keySet().size());

        // iterate over evaluated rows of the result table
        for (Integer row : key.getValues().keySet()) {
            //exclude the first two rows because the only contain the header and the datatype
            if (row == 0 || row == 1) {
                continue;
            }
            boolean isEmpty = isEmpty(targetColumn, row);
            boolean anyContains = false;
            HashMap<String, Integer> valueCounts = new HashMap<String, Integer>();

            int valCnt = 0;
            // iterate over evaluated columns
            for (TableColumn c : t.getColumns()) {
                // except the key column and the DBPedia column
                if (!isEmpty(c, row) && !c.isKey() && c != targetColumn) {
                    // the cell is not empty, hence we have found a web value
                    valCnt++;

                    // count the frequencies of evaluated values in the current row
                    String value = c.getValues().get(row);
                    int currentValueCount = 0;
                    if (valueCounts.containsKey(value)) {
                        currentValueCount = valueCounts.get(value);
                    }
                    valueCounts.put(value, currentValueCount);

                    // check if this value is contained in the DBPedia value
                    if (!isEmpty(targetColumn, row)) {
                        String dbpediaValue = targetColumn.getValues().get(row);
                        if (isMatch(dbPediadatatype, dbepdiaHeader, t, key, row, pipe, value, dbpediaValue)) {
                            anyContains = true;
                        }
                    }
                } else {
                    if (c == targetColumn) {
                        if (!written) {
                            writeTP.append(dbepdiaHeader + "\n");
                            writeFP.append(dbepdiaHeader + "\n");
                            written = true;
                        }
                        if (!isEmpty(c, row)) {
                            numDBpediaValues++;
                        }
                    }
                }
            }


            if (isEmpty) {
                if (rowNumbersToMajority.get(row) != null) {
                    //for manual evaluatioN!
                    //                   System.out.print("no DBpedia but web: " +key.getValues().get(row) + ";" + rowNumbersToMajority.get(row));
                } else {
                    //for manual evaluatioN!
                    //                 System.out.println("no value found for: " + key.getValues().get(row));
                }


                // the DBPedia value is null
                if (valCnt > 0) {
                    // but we found at least one web value
                    numRowsWithValue++;
                    numComplementaryValues++;
                }
            } else {
                // the DBPedia value is not null
                numExistingValues++;
                if (valCnt > 0) {
                    // and we found at least one web value
                    numRowsWithValue++;
                    numAdditionalValues++;
                    additionalValues.AddValue(valCnt);
                    evaluatedRows++;

                    // is this a true positive?
                    String dbpediaValue = targetColumn.getValues().get(row);
                    String webTablesValue = rowNumbersToMajority.get(row);

                    if (webTablesValue == null || dbpediaValue == null) {
                        continue;
                    }

                    if (isMatch(dbPediadatatype, dbepdiaHeader, t, key, row, pipe, webTablesValue, dbpediaValue)) {
                        writeTP.append(dbpediaValue + "\t" + webTablesValue + "\n");
                        numTruePositivesContainsSimilar++;
                    } else {
                        writeFP.append(dbpediaValue + "\t" + webTablesValue + "\n");

                    }

                    // is at least one of the values contained in the DBPedia value?
                    if (anyContains) {
                        numAnyContains++;
                    }
                }
            }
        }

        double recallBound, recall = 0, precisionSimilarContains = 0;
        if (numTotalValues == 0) {
            recallBound = 0;
        } else {
            recallBound = (double) numRowsWithValue / (double) numTotalValues;
        }
        double precision, precisionContains, precisionAnyContains, precisionSimilar;
        if (evaluatedRows
                == 0) {
            precision = 0;
            precisionAnyContains = 0;
            precisionContains = 0;
            precisionSimilar = 0;
        } else {
            System.out.println("contains: " + numTruePositivesContains + " anyContains: " + numAnyContains + " precision: " + numTruePositivesContainsSimilar + " evalRows: " + evaluatedRows + " numberRowsValue: " + numRowsWithValue);
            precision = (double) numTruePositives / (double) evaluatedRows;
            precisionContains = (double) numTruePositivesContains / (double) evaluatedRows;
            precisionAnyContains = (double) numAnyContains / (double) evaluatedRows;
            precisionSimilar = (double) numTruePositiveSimilar / (double) evaluatedRows;
            recall = (double) numTruePositivesContains / ((double) evaluatedRows + (double) numDBpediaValues);
            precisionSimilarContains = (double) numTruePositivesContainsSimilar / (double) evaluatedRows;
        }        

        lst.add(targetColumn.getHeader());
        lst.add(Integer.toString(numExistingValues));
        lst.add(Integer.toString(numRowsWithValue));
        lst.add(Integer.toString(numComplementaryValues));

        lst.add(Integer.toString(numTotalValues));
        lst.add(Integer.toString(evaluatedRows));
        lst.add(Integer.toString(numAnyContains));
        lst.add(Integer.toString(numTruePositivesContainsSimilar));


        //       lst.add(Integer.toString(numAdditionalValues));
        //       lst.add(Integer.toString((int) additionalValues.getMin()));
        //       lst.add(Integer.toString((int) additionalValues.getMax()));
        //       lst.add(Double.toString(additionalValues.getAvg()));
        lst.add(Double.toString(recallBound));
//        lst.add(Double.toString(precision));
//        lst.add(Double.toString(precisionContains));
        lst.add(Double.toString(precisionAnyContains));
//        lst.add(Double.toString(precisionSimilar));
//        lst.add(Double.toString(recall));
        lst.add(Double.toString(precisionSimilarContains));

        writeFP.flush();
        writeFP.close();
        writeTP.flush();
        writeTP.close();    

        }catch(IOException e ) {}
        return lst.toArray(new String[lst.size()]);
    }

    private boolean isMatch(TableColumn.ColumnDataType dbPediadatatype, String dbpediaHeader, Table t, TableColumn key, int row, Pipeline pipe, String webTablesValue, String dbpediaValue) {
        switch (dbPediadatatype) {
            case numeric:
                try {
                    double doubleValue = Double.parseDouble(webTablesValue.replaceAll("[^0-9\\.\\,\\-]", ""));
                    //similarity calculation for number, contains
                    if (dbpediaHeader.contains("year")) {
                        double doubleKeyValue = Double.parseDouble(dbpediaValue.split("-")[0]);
                        if (doubleValue == doubleKeyValue || doubleValue + 1 == doubleKeyValue || doubleValue == doubleKeyValue + 1) {
                            return true;
                        }
                    } else {
                        double doubleKeyValue = Double.parseDouble(dbpediaValue.replaceAll("[^0-9\\.\\,\\-]", ""));
                        double large = Math.max(Math.abs(doubleKeyValue), Math.abs(doubleValue));
                        double small = Math.min(Math.abs(doubleKeyValue), Math.abs(doubleValue));
                        double deviation = 1 - (small / large);
                        if (Math.abs(deviation) < 0.05) {
                            return true;
                        }
                    }
                } catch (Exception e) {
                }
                break;
            case date:
                try {
                    Date d1 = DateUtil.parse(webTablesValue);
                    Date d2 = DateUtil.parse(dbpediaValue);
                    int dateDiff = Math.abs(Days.daysBetween(new DateTime(d1), new DateTime(d2)).getDays());
                    //choose 360 days as the maximal difference
                    if (dateDiff < 360) {
                        return true;
                    }
                } catch (Exception e) {
                }
                break;
            case string:
                LabelBasedComparer lb = new LabelBasedColumnComparer(pipe, t);
                if (lb.matchStrings(webTablesValue, dbpediaValue) > 0.7) {
                    return true;
                }
                if (dbpediaValue.toLowerCase().contains(webTablesValue.toLowerCase()) || webTablesValue.toLowerCase().contains(dbpediaValue.toLowerCase())
                        || (abbs.containsKey(dbpediaValue.toLowerCase()) && abbs.get(dbpediaValue.toLowerCase()).equals(webTablesValue.toLowerCase()))
                        || (abbs.containsKey(webTablesValue.toLowerCase()) && abbs.get(webTablesValue.toLowerCase()).equals(dbpediaValue.toLowerCase()))
                        || (refs.containsKey(key.getValues().get(row)) && refs.get(key.getValues().get(row)).toLowerCase().contains(webTablesValue.toLowerCase()))
                        || (dictotionaryIndustry.get(dbpediaValue.toLowerCase()) != null && dictotionaryIndustry.get(dbpediaValue.toLowerCase()).contains(webTablesValue.toLowerCase()))
                        || (dictotionaryDirector.get(key.getValues().get(row)) != null && dictotionaryDirector.get(key.getValues().get(row)).contains(webTablesValue.toLowerCase()))) {
                    return true;

                }
        }
        return false;
    }

    private boolean isEmpty(TableColumn c, int row) {
        return !c.getValues().containsKey(row)
                || c.getValues().get(row) == null
                || c.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE);
    }
}
