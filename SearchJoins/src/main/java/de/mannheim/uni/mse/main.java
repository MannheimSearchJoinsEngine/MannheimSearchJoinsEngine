package de.mannheim.uni.mse;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.FileUtils;
import org.apache.lucene.search.IndexSearcher;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import de.mannheim.uni.IO.TableReader;
import de.mannheim.uni.datafusion.TableDataCleaner;
import de.mannheim.uni.evaluation.DictionaryManager;
import de.mannheim.uni.index.IndexManager;
import de.mannheim.uni.model.IndexEntry;
import de.mannheim.uni.model.JoinResult;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.schema.ColumnScoreValue;
import de.mannheim.uni.model.schema.TableObjectsMatch;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.Matcher;
import de.mannheim.uni.schemamatching.SimilarTermsGenerator;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.searchjoin.SearchJoin;
import de.mannheim.uni.statistics.ComplementarityAnalyser;
import de.mannheim.uni.statistics.DistributionOfTablesPerKey;
import de.mannheim.uni.statistics.DistributionOfValuesPerKey;
import de.mannheim.uni.statistics.EvaluatedJoinResult;
import de.mannheim.uni.statistics.SearchResultOverview;
import de.mannheim.uni.statistics.SearchTableResultAnalyzer;
import de.mannheim.uni.statistics.TableAnalyser;
import de.mannheim.uni.statistics.Timer;
import de.mannheim.uni.statistics.Web2DBPediaAnalyser;
import de.mannheim.uni.utils.FastJoinWrapper;
import de.mannheim.uni.utils.PipelineConfig;

public class main {

    private static Options getCommandLineOptions() {
        /* set up command line parameters */
        Options options = new Options();

        Option conf = OptionBuilder.withArgName("conf")
                .hasArg()
                .withDescription("Path to config file")
                .create("conf");
        /*
         // Credentials
         Option accessKey = OptionBuilder.withArgName("accesskey")
         .hasArg()
         .withDescription("Your S3 Access Key")
         .create("accesskey");
		
		
         Option secretKey = OptionBuilder.withArgName("secret")
         .hasArg()
         .withDescription("Your S3 Secret")
         .create("secret"); 
		
         // Command target
         Option targetFile = OptionBuilder.withArgName("file")
         .hasArg()
         .withDescription("A file containing the S3-URLs of all files to process")
         .create("targetfile");
         Option targetS3 = OptionBuilder.withArgName("URL")
         .hasArg()
         .withDescription("A URL representing the filter to determine the S3-files to process")
         .create("targetfilter");
		
         // get command
         options.addOption("overwritelocal", false, "Overwrite local files");
         options.addOption("requesterpays", false, "Enabled download from 'requester pays' sources");
		
         //Option cmdTarget = OptionBuilder.withArgName(name)
		
         // General options
         int numThreads = Runtime.getRuntime().availableProcessors();
		
		
         options.addOption(accessKey);
         options.addOption(secretKey);
         options.addOption(targetFile);
         options.addOption(targetS3);*/

        options.addOption(conf);

        return options;
    }

    public static void main(String[] args) {
        /* parse command line parameters */
        CommandLineParser parser = new BasicParser();
        CommandLine cmd;

        try {
            cmd = parser.parse(getCommandLineOptions(), args);

            Timer tim = new Timer(cmd.getArgs()[0]);

            String conf = cmd.getOptionValue("conf", "searchjoins.conf");

            //String arg = cmd.getOptionValue("arg");
            Pipeline p = Pipeline.getPipelineFromConfigFile("default", conf);

            //System.out.println("*** " + cmd.getArgs()[0] + " +" + (cmd.getArgs().length-1) + " ***");

            if (cmd.getArgs()[0].equals("join") && cmd.getArgs().length == 6) {
                String t1 = cmd.getArgs()[1];
                String t1type = cmd.getArgs()[2];
                String t2 = cmd.getArgs()[3];
                String t2type = cmd.getArgs()[4];
                String tout = cmd.getArgs()[5];

                if (tout.endsWith(".csv")) {
                    tout = tout.substring(0, tout.lastIndexOf('.'));
                }

                Join j = new Join(p);
                TableReader tr = new TableReader(p);

                System.out.println("Reading table 1 ...");
                Table tLeft = null;
                if (t1type.equals("dbpedia")) {
                    tLeft = tr.readLodTable(t1);
                } else if (t1type.equals("web")) {
                    tLeft = tr.readWebTable(t1);
                } else if (t1type.equals("search")) {
                    tLeft = tr.readQueryTable(t1);
                }

                if (tLeft == null) {
                    System.out.println("Unknown table type: " + t1type);
                    return;
                }

                System.out.println("Reading table 2 ...");
                Table tRight = null;

                if (t2type.equals("dbpedia")) {
                    tRight = tr.readLodTable(t1);
                } else if (t2type.equals("web")) {
                    tRight = tr.readWebTable(t1);
                } else if (t2type.equals("search")) {
                    tRight = tr.readQueryTable(t1);
                }

                if (tRight == null) {
                    System.out.println("Unknown table type: " + t2type);
                    return;
                }

                System.out.println("Joining tables ...");
                Table tOut = j.joinTables(tLeft, tRight);

                System.out.println("Writing output ...");
                tOut.writeTableToFile(tout, false);

                System.out.println("Done.");
            } else if (cmd.getArgs()[0].equals("clean") && cmd.getArgs().length == 3) {
                String t = cmd.getArgs()[1];
                String tout = cmd.getArgs()[2];

                if (tout.endsWith(".csv")) {
                    tout = tout.substring(0, tout.lastIndexOf('.'));
                }

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table ta = tr.readTable(t);

                System.out.println("Removing empty columns ...");
                tr.removeEmptyCols(ta);

                System.out.println("Removing emtpy rows ...");
                tr.removeEmptyRows(ta);

                System.out.println("Writing output ...");
                ta.writeTableToFile(tout, false);

                System.out.println("Done.");
            } else if (cmd.getArgs()[0].equals("match") && cmd.getArgs().length == 4) {
                String t1 = cmd.getArgs()[1];
                String t2 = cmd.getArgs()[2];
                String tout = cmd.getArgs()[3];

                if (tout.endsWith(".csv")) {
                    tout = tout.substring(0, tout.lastIndexOf('.'));
                }

                Join j = new Join(p);
                TableReader tr = new TableReader(p);

                System.out.println("Reading table 1 ...");
                Table tLeft = tr.readTable(t1);
                System.out.println("Reading table 2 ...");
                Table tRight = tr.readTable(t2);

                /*System.out.println("Joining tables ...");
                 Table ta = j.joinTables(tLeft, tRight);
				
                 System.out.println("Matching columns ...");
                 TableDataCleaner c = new TableDataCleaner(ta, p);
                 ta = c.cleanTable();*/

                SearchJoinExecutor sje = new SearchJoinExecutor(p);

                Table ta = sje.joinAndMatchTables(tLeft, tRight);

                System.out.println("Writing output ...");
                ta.writeTableToFile(tout, false);

                System.out.println("Done.");
            } else if (cmd.getArgs()[0].equals("matchTable") && cmd.getArgs().length == 3) {
                String t1 = cmd.getArgs()[1];
                String tout = cmd.getArgs()[2];

                if (tout.endsWith(".csv")) {
                    tout = tout.substring(0, tout.lastIndexOf('.'));
                }

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table t = tr.readTable(t1);

                int i = 0;
                TableColumn dbpc = null;
                for (TableColumn c : t.getColumns()) {
                    c.setDataSource(Integer.toString(i++));
                    if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                        dbpc = c;
                    }
                }

                TableDataCleaner dataCleaner = new TableDataCleaner(t, p);

                if (dbpc != null) {
                    dataCleaner.removeNullRows(t, dbpc);
                }

                t = dataCleaner.cleanTable();

                t.writeTableToFile(tout);

                /*
                 Matcher m = new Matcher(p);
				
                 try {
                 HashMap<TableColumn, HashMap<TableColumn, ColumnScoreValue>> scores =  m.matchInstances(t);
					
                 for(TableColumn c1 : scores.keySet())
                 {
                 for(TableColumn c2 : scores.get(c1).keySet())
                 {
                 ColumnScoreValue s = scores.get(c1).get(c2);
							
                 //System.out.println(s.getAverage());
                 }
                 }
					
                 } catch (InterruptedException e) {
                 // TODO Auto-generated catch block
                 e.printStackTrace();
                 }*/
            } else if (cmd.getArgs()[0].equals("analyse") && cmd.getArgs().length == 3) {
                String t = cmd.getArgs()[1];

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table tt = tr.readTable(t);

                //TableAnalyser ta = new TableAnalyser();
                //ta.analyseTable(tt, out);
                System.out.println("generating distribution of values per key");
                DistributionOfValuesPerKey d = new DistributionOfValuesPerKey();
                d.analyse(tt, p, null);

                System.out.println("Evaluating search results");
                List<JoinResult> results = JoinResult.readCsv(args[2]);

                DistributionOfTablesPerKey d2 = new DistributionOfTablesPerKey();
                d2.analyse(results, p);

                // find dbpedia column
                TableColumn dbpc = null;
                for (TableColumn c : tt.getColumns()) {
                    if (c.getHeader().contains("dbpedia_")) {
                        dbpc = c;
                        break;
                    }
                }

                if (dbpc != null) {
                    System.out.println("Evaluating complementarity and evidence for DBPedia column " + dbpc.getHeader());

                    ComplementarityAnalyser ca = new ComplementarityAnalyser();
                    ca.analyse(tt, dbpc, p);
                }
            } else if (cmd.getArgs()[0].equals("analyse_dbpc") && cmd.getArgs().length == 2) {
                String t = cmd.getArgs()[1];

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table tt = tr.readTable(t);

                // find dbpedia column
                TableColumn dbpc = null;
                for (TableColumn c : tt.getColumns()) {
                    if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                        dbpc = c;
                        break;
                    }
                }

                if (dbpc != null) {
                    System.out.println("generating distribution of values per key");
                    DistributionOfValuesPerKey d = new DistributionOfValuesPerKey();
                    d.analyse(tt, p, dbpc);

                    System.out.println("Evaluating complementarity and evidence for DBPedia column " + dbpc.getHeader());

                    ComplementarityAnalyser ca = new ComplementarityAnalyser();
                    ca.analyse(tt, dbpc, p);
                }
            } else if (cmd.getArgs()[0].equals("overview") && cmd.getArgs().length >= 2) {
                String out = cmd.getArgs()[1];

                List<String> in = new LinkedList<String>();

                for (int i = 2; i < cmd.getArgs().length; i++) {
                    in.add(cmd.getArgs()[i]);
                }

                String[] paths = in.toArray(new String[]{});

                SearchResultOverview o = new SearchResultOverview();
                o.generate(paths, p, out);
            } else if (cmd.getArgs()[0].equals("web2dbp") && cmd.getArgs().length >= 2) {
                List<String> in = new LinkedList<String>();

                for (int i = 3; i < cmd.getArgs().length; i++) {
                    in.add(cmd.getArgs()[i]);
                }

                HashMap<String, Table> tbls = new HashMap<String, Table>();
                HashMap<String, TableColumn> cols = new HashMap<String, TableColumn>();
                boolean augmented = false;
                TableReader reader = new TableReader();
                for (String s : in) {

                    Table t;
                    TableColumn dbpc = null;

                    if (args[1].equals("full")) {
                        t = reader.readTableFast(new File(s, "FullTable.csv.gz").getAbsolutePath());
                    } else {
                        t = reader.readTableFast(new File(s, "AugmentedTable.csv").getAbsolutePath());
                        augmented = true;
                    }
                    if (args[2].equals("change")) {
                        Table queryTable = reader.readTable(new File(s, "company.gz").getAbsolutePath());

                        TableColumn dbpediaValuesOld = null;
                        for (TableColumn c : t.getColumns()) {
                            if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                                dbpediaValuesOld = c;
                                break;
                            }
                        }

                        TableColumn dbpediaValuesNew = null;
                        String newHeader = "";
                        for (TableColumn c : queryTable.getColumns()) {
                            if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                                dbpediaValuesNew = c;
                                newHeader = c.getHeader();
                                break;
                            }
                        }

                        TableColumn keyOld = t.getFirstKey();
                        TableColumn keyNew = queryTable.getFirstKey();
                        for (Integer row : keyOld.getValues().keySet()) {
                            String currentKey = keyOld.getValues().get(row);
                            String newValue = "";
                            for (Integer rowNew : keyNew.getValues().keySet()) {
                                if (keyNew.getValues().get(rowNew).equals(currentKey)) {
                                    newValue = dbpediaValuesNew.getValues().get(rowNew);
                                    break;
                                }
                            }
                            if (newValue == null) {
                                dbpediaValuesOld.addNewValue(row, PipelineConfig.NULL_VALUE, true);
                            } else {
                                dbpediaValuesOld.addNewValue(row, newValue, true);
                            }
                        }

                        for (TableColumn c : t.getColumns()) {
                            if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                                c.setHeader(newHeader);
                                c.setDataType(dbpediaValuesNew.getDataType());
                                dbpc = c;
                                break;
                            }
                        }

                    } else {
                        for (TableColumn c : t.getColumns()) {
                            if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                                dbpc = c;
                                break;
                            }
                        }
                    }
                    if (dbpc != null) {
                        tbls.put(s, t);
                        cols.put(s, dbpc);                        
                    }
                    else {
                        System.out.println("No DBPedia column in " + s);
                        tbls.put(s, t);
                        dbpc = new TableColumn();
                        cols.put(s, dbpc);
                    }
                    

                }
                System.out.println("*** " + tbls.size() + " tables ***");

                Web2DBPediaAnalyser a = new Web2DBPediaAnalyser();
                if (augmented) {
                    a.analyze(tbls, cols, p, true);
                } else {
                    a.analyze(tbls, cols, p, false);
                }
                //a.analyseMajority(tbls, cols, p);
            } else if (cmd.getArgs()[0].equals("printJoinTables") && cmd.getArgs().length == 2) {
                String t = cmd.getArgs()[1];

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table tt = tr.readTable(t);

                SearchJoin sj = new SearchJoin(p);
                Set<String> result = sj.printJoinTablesForColumnFast(tt.getFirstKey(), tt);

                for (String s : result) {
                    System.out.println("INFO: -----------------------------------------------" + s + "-----------------------------------------------");
                }
            } else if (cmd.getArgs()[0].equals("createValueHeaderFilter") && cmd.getArgs().length == 3) {
                String t = cmd.getArgs()[1];
                String out = cmd.getArgs()[2];

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table tt = tr.readTable(t);

                TableColumn dbpc = null;
                for (TableColumn c : tt.getColumns()) {
                    if (c.getHeader().contains("dbpedia_") && !c.isKey()) {
                        dbpc = c;
                        break;
                    }
                }

                if (dbpc != null) {
                    System.out.println("Using column " + dbpc.getHeader());

                    TreeSet<String> values = new TreeSet<String>();

                    for (Integer row : tt.getFirstKey().getValues().keySet()) {
                        if (dbpc.getValues().containsKey(row) && !dbpc.getValues().get(row).equalsIgnoreCase(PipelineConfig.NULL_VALUE)) {
                            values.add(dbpc.getValues().get(row));
                        }
                    }

                    FileUtils.writeLines(new File(out), values);
                } else {
                    System.out.println("No DBPedia column in " + t);
                }
            } else if (cmd.getArgs()[0].equals("createSimilarHeaderFilter") && cmd.getArgs().length == 2) {
                String out = cmd.getArgs()[1];

                //System.out.println("Reading table ...");

                HashSet<String> headers = new HashSet<String>();
                List<String> initialHeaders = (List<String>) FileUtils.readLines(new File(out));

                SimilarTermsGenerator simgen = new SimilarTermsGenerator(p);

                for (String s : initialHeaders) {
                    headers.add(s);

                    List<String> results = simgen.getWordnetTerms(s, true, false, false);
                    headers.addAll(results);

                    for (String value : results) {
                        System.out.println("Adding " + value);
                    }
                }

                FileUtils.writeLines(new File(out), headers);
            } else if (cmd.getArgs()[0].equals("extractHeadersFromIndex") && cmd.getArgs().length == 2) {
                String out = cmd.getArgs()[1];

                IndexManager im = new IndexManager(p);

                Set<String> headers = im.getTableHeaders();

                System.out.println("Found " + headers.size() + " different headers");

                FileUtils.writeLines(new File(out), headers);
            } else if (cmd.getArgs()[0].equals("normaliseString") && cmd.getArgs().length == 2) {
                String value = cmd.getArgs()[1];

                value = StringNormalizer.clearString(value, true, p.isSearchStemmedColumnHeaders());

                System.out.println(value);
            } else if (cmd.getArgs()[0].equals("normaliseFile") && cmd.getArgs().length == 2) {
                String value = cmd.getArgs()[1];

                List<String> lines = FileUtils.readLines(new File(value));

                for (String s : lines) {
                    System.out.println(StringNormalizer.clearString(s, true, p.isSearchStemmedColumnHeaders()));
                }
            } else if (cmd.getArgs()[0].equals("prepareFastJoin") && cmd.getArgs().length >= 3) {
                String file = cmd.getArgs()[1];
                String out = cmd.getArgs()[2];

                boolean writeAll = false;
                if (cmd.getArgs().length > 3) {
                    writeAll = Boolean.parseBoolean(cmd.getArgs()[3]);
                }

                BufferedReader r = new BufferedReader(new FileReader(file));
                BufferedWriter w = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(out),
                        Charset.forName("ASCII").newEncoder()));
                BufferedWriter wE = new BufferedWriter(new FileWriter(out + ".errors"));

                String line = null;

                while ((line = r.readLine()) != null) {
                    String value = line;

                    // change string to conform to FastJoin limitations

                    // only ASCI?
                    value = value.replaceAll("\\P{InBasic_Latin}", "");

                    // max 128 characters
                    value = value.substring(0, Math.min(value.length(), 127));

                    //System.out.println(value);
                    try {
                        if (value.trim().length() > 0 || writeAll) {
                            w.write(value + "\n");
                        }
                    } catch (Exception e) {
                        wE.write(value + ": " + e.getMessage() + "\n");
                    }

                }

                r.close();
                w.close();
                wE.close();
            } else if (cmd.getArgs()[0].equals("getOriginalHeaders") && cmd.getArgs().length >= 3) {
                System.out.println("getOriginalHeaders +" + cmd.getArgs().length);

                String orig = cmd.getArgs()[1];
                String fj = cmd.getArgs()[2];

                //String output = cmd.getArgs()[4];

                HashMap<String, List<Integer>> fj_map = new HashMap<String, List<Integer>>();
                HashMap<Integer, String> orig_map = new HashMap<Integer, String>();

                // read normalized and stemmed values as keys and row numbers as values
                System.out.println("Reading all normalized headers");
                BufferedReader r = new BufferedReader(new FileReader(fj));

                String line = null;
                int row = 0;
                while ((line = r.readLine()) != null) {
                    if (!fj_map.containsKey(line)) {
                        fj_map.put(line, new LinkedList<Integer>());
                    }

                    List<Integer> lst = fj_map.get(line);
                    lst.add(row++);
                }

                r.close();

                // read all row numbers as keys and original values as values
                System.out.println("Reading all original headers");
                r = new BufferedReader(new FileReader(orig));

                row = 0;

                while ((line = r.readLine()) != null) {
                    orig_map.put(row++, line);
                }

                r.close();

                for (int i = 3; i < cmd.getArgs().length; i++) {
                    String input = cmd.getArgs()[i];
                    // translate all normalized and stemmed headers into original values
                    System.out.println("translating selected headers");
                    r = new BufferedReader(new FileReader(input));
                    List<String> results = new LinkedList<String>();

                    while ((line = r.readLine()) != null) {
                        if (fj_map.containsKey(line)) {
                            for (Integer j : fj_map.get(line)) {
                                //w.write(orig_map.get(i) + "\n");
                                results.add(orig_map.get(j));
                            }
                        }
                    }

                    r.close();

                    BufferedWriter w = new BufferedWriter(new FileWriter(input));

                    for (String s : results) {
                        w.write(s + "\n");
                    }

                    w.close();
                }
            } else if (cmd.getArgs()[0].equals("getKeyOverlap") && cmd.getArgs().length == 3) {
                String t = cmd.getArgs()[1];
                String out = cmd.getArgs()[2];

                TableReader tr = new TableReader(p);

                System.out.println("Reading table ...");
                Table tt = tr.readTable(t);

                SearchJoin sj = new SearchJoin(p);
                Set<String[]> result = sj.getKeyOverlap(tt.getFirstKey(), tt);

                CSVWriter w = new CSVWriter(new FileWriter(out));

                for (String[] s : result) {
                    w.writeNext(s);
                }

                w.close();

                BufferedWriter bw = new BufferedWriter(new FileWriter("fj_" + out));

                for (String[] s : result) {
                    String norm = StringNormalizer.clearString(s[0], true, p.isSearchStemmedColumnHeaders());
                    bw.write(norm);
                }

                bw.close();

                HashMap<String, String[]> map = new HashMap<String, String[]>();
                for (String[] s : result) {
                    map.put(s[0], s);
                }
            } else if (cmd.getArgs()[0].equals("getFJResults") && cmd.getArgs().length == 3) {
                FastJoinWrapper fjw = new FastJoinWrapper();
                fjw.extractMatches(Double.parseDouble(cmd.getArgs()[1]), cmd.getArgs()[2]);
            } else if (cmd.getArgs()[0].equals("getFJMatching") && cmd.getArgs().length == 4) {
                String lst = cmd.getArgs()[1];
                String fj = cmd.getArgs()[2];
                String out = cmd.getArgs()[3];

                List<String> matches = FileUtils.readLines(new File(fj));

                CSVReader r = new CSVReader(new FileReader(lst));
                CSVWriter w = new CSVWriter(new FileWriter(out));

                String[] values = null;

                while ((values = r.readNext()) != null) {
                    String norm = StringNormalizer.clearString(values[0], true, p.isSearchStemmedColumnHeaders());
                    if (matches.contains(norm)) {
                        w.writeNext(values);
                    }
                }

                r.close();

            } else if (cmd.getArgs()[0].equals("compareDouble") && cmd.getArgs().length >= 4) {
                String first = cmd.getArgs()[1];
                int firstCol = Integer.parseInt(cmd.getArgs()[2]);
                LinkedList<Double> firstValues = new LinkedList<Double>();

                CSVReader r = new CSVReader(new FileReader(first), '\t');
                r.readNext(); // skip header

                String[] values = null;

                while ((values = r.readNext()) != null) {
                    firstValues.add(Double.parseDouble(values[firstCol]));
                }
                r.close();

                for (int i = 3; i < cmd.getArgs().length; i += 2) {
                    String second = cmd.getArgs()[i];
                    int secondCol = Integer.parseInt(cmd.getArgs()[i + 1]);
                    double dev = 0;

                    r = new CSVReader(new FileReader(second), '\t');
                    r.readNext();

                    int row = 0;
                    while ((values = r.readNext()) != null) {
                        if (row == firstValues.size()) {
                            break;
                        }

                        Double value = Double.parseDouble(values[secondCol]);

                        dev += Math.abs(value - firstValues.get(row++));
                    }

                    System.out.println(second + ": " + (dev / row));
                }
            } else if (cmd.getArgs()[0].equals("createDictionary") && cmd.getArgs().length == 4) {
                String valuesPath = cmd.getArgs()[1];
                String dictionaryPath = cmd.getArgs()[2];
                String header = cmd.getArgs()[3];

                DictionaryManager dm = new DictionaryManager(p);

                dm.buildDictionary(valuesPath, dictionaryPath, header);
            } else if (cmd.getArgs()[0].equals("createDictionaryFromTable") && cmd.getArgs().length >= 3) {
                String tablePath = cmd.getArgs()[1];
                String dictionaryPath = cmd.getArgs()[2];


                TableReader tr = new TableReader(p);
                Table t = tr.readTableFast(tablePath);
                Table to = null;

                if (cmd.getArgs().length > 3) {
                    String origTablePath = cmd.getArgs()[3];
                    to = tr.readQueryTableFast(origTablePath, true);
                }

                String script = null;
                if (cmd.getArgs().length > 4) {
                    script = cmd.getArgs()[4];
                }

                DictionaryManager dm = new DictionaryManager(p);

                dm.buildDictionaryFromTable(t, dictionaryPath, to, script);
            } else if (cmd.getArgs()[0].equals("printDictionary") && cmd.getArgs().length == 2) {
                String dictionaryPath = cmd.getArgs()[1];

                DictionaryManager dm = new DictionaryManager(p);

                Map<String, Collection<String>> dic = dm.loadDictionary(dictionaryPath);

                for (String key : dic.keySet()) {
                    System.out.println("Key: " + key);
                    System.out.print("Values: ");

                    boolean first = true;
                    for (String value : dic.get(key)) {
                        if (!first) {
                            System.out.print(", ");
                        }

                        System.out.print(value);

                        first = false;
                    }

                    System.out.println();
                    System.out.println();
                }

            } else if (cmd.getArgs()[0].equals("testArgs")) {
                System.out.println(cmd.getArgs().length);

                for (int i = 0; i < cmd.getArgs().length; i++) {
                    System.out.println(i + ": " + cmd.getArgs()[i]);
                }
            }

            tim.stop();
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }
    }
}
