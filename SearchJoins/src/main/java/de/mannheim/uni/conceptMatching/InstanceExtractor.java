/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.mannheim.uni.conceptMatching;

import de.mannheim.uni.utils.CsvConverter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 * @author domi
 */
public class InstanceExtractor {

    public static String name = "Films";
    public static String type = "Random";    

    public static void main(String args[]) throws FileNotFoundException, IOException {

//        deleteDuplicates(new File("C:\\Users\\domi\\tmp\\Companies\\TopCompaniesAll.csv"), new File("C:\\Users\\domi\\tmp\\Companies\\TopCompanies.csv"));

//        nullCounterPerColumn();
        
        

        File wholeTable = new File("C:\\Users\\domi\\tmp\\" + name + "\\"+type+"\\DBpedia" + name + "Final.csv");
        
 //      File wholeTable = new File("C:\\Users\\domi\\tmp\\" + name + "\\All\\" + name + "SelectedTab.txt");
        //      File listOfInstances = new File("C:\\Users\\domi\\tmp\\" + name + "\\"+type+"\\TopCompaniesAll.csv");
        //     File out = extractInstancesWithList(wholeTable, listOfInstances);
 //      File out = extractRandomInstances(wholeTable, 500);
//        File out = extractInstancesWithAttribute(wholeTable, 11, 2010);

        //       joinTables(wholeTable, listOfInstances);
        
        String[] filmAttribues = new String[]{"runtime","budget","cinematography","country","director","distributor","editing","gross","musicComposer","producer","releaseDate","starring","writer"};
        String[] countryAttribues = new String[]{"areaTotal","capital","currency","governmentType","language","leaderName","leaderTitle","longName","officialLanguage","populationDensity","anthem","foundingDate","largestCity","motto"};
        String[] companyAttribues = new String[]{"assets","equity","netIncome","numberOfEmployees","operatingIncome","revenue","foundationPlace","foundedBy","industry","keyPerson","locationCity","locationCountry","headquarter","legalForm","foundingYear","type","successor"};
        
        for(String s: filmAttribues) {
        
        File out = new File("C:\\Users\\domi\\tmp\\" + name + "\\" + type + "\\SingleAttribute\\" + name + "_"+s+""+".csv");
        spliTableByAttribute(wholeTable, out, s);
        
//
//        makeEntriesUnique(new File("C:\\Users\\domi\\tmp\\"+name+"Own\\"+name+"Out.csv"), new File("C:\\Users\\domi\\tmp\\"+name+"Own\\"+name+"OutUnique.csv"));
        CsvConverter convert = new CsvConverter();
        convert.convertCsv(out.getPath(), "C:\\Users\\domi\\tmp\\" + name + "\\" + type + "\\SingleAttribute\\" + name + "_"+s+""+".gz");
  //      convert.convertCsv(out.getPath(), "C:\\Users\\domi\\tmp\\" + name + "\\"+type+"\\" + name + "DBpedia.gz");
        }
    }

    private static void makeEntriesUnique(File in, File out) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        BufferedReader readT1 = new BufferedReader(new InputStreamReader(new FileInputStream(in), "UTF-8"));
        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "UTF-8"));
        String line = readT1.readLine();
        int i = 0;
        while (line != null) {
            if (i == 0) {
                String[] headerValues = line.split(";");
                String newLine = "";
                for (int j = 0; j < headerValues.length; j++) {
                    newLine += headerValues[j] + " " + j + ";";
                }
                i++;
                writeTable.append(newLine + "\n");
                line = readT1.readLine();
                continue;
            } else {
                i++;
                writeTable.append(line + "\n");
                line = readT1.readLine();
            }
        }
    }

    private static File spliTableByAttribute(File table, File out, String attributeName) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        BufferedReader readT1 = new BufferedReader(new InputStreamReader(new FileInputStream(table), "UTF-8"));
        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "UTF-8"));
        String line = readT1.readLine();
        boolean header = true;
        char delimiter = CsvConverter.determineDelimiter(table.getPath());
        int attributeIndex =0;
        while (line != null) {  
            if(header) {
                String[] entries = line.split(""+delimiter+"");
                header = false;
                String headerString ="";
                for (int i = 0; i < entries.length; i++) {                    
                    if(i==0) {
                        headerString += entries[i]+ "" + delimiter + "";
                    }
                    else if(entries[i].toLowerCase().contains(attributeName.toLowerCase())) {
                        headerString += entries[i];
                        attributeIndex = i;
                        writeTable.write(headerString+"\n");
                        break;
                    }
                }
                line = readT1.readLine();
                continue;
            }
            String[] entries = line.split(""+delimiter+"");
            String lineContent = "";
            for (int i = 0; i < entries.length; i++) {
                    if(i==0) {
                        lineContent += entries[i]+ "" + delimiter + "";
                    }
                    else if(i==attributeIndex) {
                        lineContent += entries[i];
                        writeTable.write(lineContent+"\n");
                        break;
                    }
                }
            line = readT1.readLine();
        }        
        writeTable.flush();
        writeTable.close();
        return out;
    }
    
    
    private static File joinTables(File t1, File t2) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        File tableOut = new File("C:\\Users\\domi\\tmp\\" + name + "\\Top\\" + name + "Out.csv");
        char delimiter = CsvConverter.determineDelimiter(t1.getPath());
        int keyColumn = 0;
        BufferedReader readT1 = new BufferedReader(new InputStreamReader(new FileInputStream(t1), "UTF-8"));
        BufferedReader readT2 = new BufferedReader(new InputStreamReader(new FileInputStream(t2), "UTF-8"));
        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableOut), "UTF-8"));
        Map<String, List<String>> keyToValues = new HashMap<String, List<String>>();
        List<String> keysToWrite = new ArrayList<String>();
        String headerString = "";
        int firstTableCounter = 0;

        String line = readT1.readLine();
        boolean header = true;
        while (line != null) {
            if (header) {
                String[] headerValues = line.split("" + delimiter + "");
                firstTableCounter = headerValues.length;
                for (int i = 0; i < headerValues.length; i++) {
                    headerString += headerValues[i] + "" + delimiter + "";
                }
                header = false;
                line = readT1.readLine();
                continue;
            }
            String[] entries = line.split("" + delimiter + "");
            List x = new ArrayList();
            x.addAll(Arrays.asList(entries));
            if (!keyToValues.containsKey(entries[keyColumn].toLowerCase())) {
                keyToValues.put(entries[keyColumn].toLowerCase(), x);
            }
            line = readT1.readLine();
        }
        header = true;

        line = readT2.readLine();
        while (line != null) {
            if (header) {
                String[] headerValues = line.split("" + delimiter + "");
                for (int i = 1; i < headerValues.length; i++) {
                    if (i == headerValues.length - 1) {
                        headerString += headerValues[i];
                    } else {
                        headerString += headerValues[i] + "" + delimiter + "";
                    }
                }
                header = false;
                line = readT2.readLine();
                continue;
            }
            String[] entries = line.split("" + delimiter + "");
            if (keyToValues.containsKey(entries[0].toLowerCase())) {
                List<String> values = new ArrayList<String>();
                for (int i = 1; i < entries.length; i++) {
                    values.add(entries[i]);
                }
                if (keyToValues.get(entries[0].toLowerCase()).size() < firstTableCounter + 10) {
                    keyToValues.get(entries[0].toLowerCase()).addAll(values);
                }
                if (!keysToWrite.contains(entries[0].toLowerCase())) {
                    keysToWrite.add(entries[0].toLowerCase());
                }
            }
            line = readT2.readLine();
        }
        writeTable.write(headerString + "\n");

        for (String key : keyToValues.keySet()) {
            String lineToWrite = "";
            if (keysToWrite.contains(key.toLowerCase())) {
                lineToWrite += key.toLowerCase() + "" + delimiter + "";
                for (String value : keyToValues.get(key)) {
                    lineToWrite += value + "" + delimiter + "";
                }
                if (lineToWrite.endsWith("" + delimiter + "")) {
                    lineToWrite = lineToWrite.substring(0, lineToWrite.length() - 1);
                }
                writeTable.write(lineToWrite + "\n");
            }
        }

        return null;
    }

    private static void nullCounterPerColumn() throws IOException {
        File wholeTable = new File("C:\\Users\\domi\\tmp\\Countries\\CountriesTrans.csv");
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(wholeTable), "UTF-8"));
        String line = readTable.readLine();
        while (line != null) {
            int nullcounter = 0, all = 0;

            String[] newEntries = line.split("\t");
            for (int j = 0; j < newEntries.length; j++) {
                if (newEntries[j].toLowerCase().equals("null") || newEntries[j].isEmpty()) {
                    nullcounter++;
                }
                all++;
            }
            System.out.println(newEntries[0] + "\t" + all + "\t" + nullcounter);
            line = readTable.readLine();
        }
    }

    private static void deleteDuplicates(File in, File out) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        BufferedReader readT1 = new BufferedReader(new InputStreamReader(new FileInputStream(in), "UTF-8"));
        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "UTF-8"));
        Set<String> names = new HashSet<String>();
        String line = readT1.readLine();
        while (line != null) {
            names.add(line.toLowerCase());
            line = readT1.readLine();
        }
        for (String uniqueName : names) {
            writeTable.write(uniqueName + "\n");
        }
        writeTable.flush();
        writeTable.close();
    }

    private static void nullCounter() throws FileNotFoundException, UnsupportedEncodingException, IOException {
        File wholeTable = new File("C:\\Users\\domi\\tmp\\CountriesOwn\\DBpediaSelectedCountries.csv");
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(wholeTable), "UTF-8"));
        ArrayList<String> ontoProps = new ArrayList<String>();
        ontoProps.add("dbpedia_abstract");
        ontoProps.add("dbpedia_areaTotal");
        ontoProps.add("dbpedia_capital");
        ontoProps.add("dbpedia_currency");
        ontoProps.add("dbpedia_demonym");
        ontoProps.add("dbpedia_foundingDate");
        ontoProps.add("dbpedia_governmentType");
        ontoProps.add("dbpedia_language");
        ontoProps.add("dbpedia_largestCity");
        ontoProps.add("dbpedia_leaderName");
        ontoProps.add("dbpedia_leaderTitle");
        ontoProps.add("dbpedia_longName");
        ontoProps.add("dbpedia_officialLanguage");
        ontoProps.add("dbpedia_motto");
        ontoProps.add("dbpedia_percentageOfAreaWater");
        ontoProps.add("dbpedia_populationDensity");
        List<Integer> pos = new ArrayList<Integer>();

        String line = readTable.readLine();
        int i = 0, nullcounter = 0, all = 0;
        while (line != null) {
            if (i == 0) {
                String[] newEntries = line.split(";");
                for (int j = 0; j < newEntries.length; j++) {
                    if (ontoProps.contains(newEntries[j])) {
                        pos.add(j);
                    }
                }

                line = readTable.readLine();
                i++;
                continue;
            }
            if (pos.contains(i)) {
                String[] newEntries = line.split(";");
                for (int j = 0; j < newEntries.length; j++) {
                    if (newEntries[j].toLowerCase().equals("null") || newEntries[j].isEmpty()) {
                        nullcounter++;
                    }
                    all++;
                }
            }
            line = readTable.readLine();
        }
        System.out.println(nullcounter + " " + all);
    }

    private static File extractRandomInstances(File table, int number) throws IOException {
        File tableOut = new File("C:\\Users\\domi\\tmp\\" + name + "\\Random\\DBpedia" + name + "Final.csv");
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(table), "UTF-8"));
        BufferedReader countTable = new BufferedReader(new InputStreamReader(new FileInputStream(table), "UTF-8"));
        int counter = 0;
        String line = countTable.readLine();
        while (line != null) {
            counter++;
            line = countTable.readLine();
        }
        Set<Integer> usedLines = new HashSet<Integer>();

        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableOut), "UTF-8"));
        boolean header = true;
        for (int i=1; i<number; i++) {
            //random number between 1 and number
            int randomLine = (int) (Math.random() * counter + 1);
            if (!usedLines.contains(randomLine)) {
                usedLines.add(randomLine);
            }
            else {
                i--;
            }
        }

        line = readTable.readLine();
        int lineNumber = 0;
        while (line != null) {
            if (header) {
                writeTable.write(line + "\n");
                header = false;
                line = readTable.readLine();
                lineNumber++;
                continue;
            }
            if (usedLines.contains(lineNumber)) {
                writeTable.write(line + "\n");
                usedLines.add(lineNumber);
            }
            lineNumber++;
            line = readTable.readLine();
        }
        writeTable.flush();
        writeTable.close();
        return tableOut;
    }

    private static File extractInstancesWithAttribute(File table, int selectColumn, int value) throws IOException {
        File tableOut = new File("C:\\Users\\domi\\tmp\\" + name + "\\Top\\DBpedia" + name + "Final.csv");
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(table), "UTF-8"));
        //release date
        char delimiter = CsvConverter.determineDelimiter(table.getPath());

        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableOut), "UTF-8"));
        String line = readTable.readLine();
        boolean header = true;
        while (line != null) {
            if (header) {
                writeTable.write(line + "\n");
                header = false;
                line = readTable.readLine();
                continue;
            }
            String[] entries = line.split("" + delimiter + "");
            if (entries[selectColumn].isEmpty() || entries[selectColumn].toLowerCase().equals("null") || entries[selectColumn].contains("{") || entries[selectColumn].split("\\.").length < 2) {
                line = readTable.readLine();
                continue;
            }
            int year = Integer.parseInt(entries[selectColumn].split("\\.")[2]);
            if (year > value) {
                writeTable.write(line + "\n");
            }
            line = readTable.readLine();
        }
        writeTable.flush();
        writeTable.close();
        return tableOut;
    }

    private static File extractInstancesWithList(File table, File listOfInstances) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        File tableOut = new File("C:\\Users\\domi\\tmp\\" + name + "\\Top\\DBpedia" + name + "Final.csv");
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(table), "UTF-8"));
        BufferedReader readInstances = new BufferedReader(new InputStreamReader(new FileInputStream(listOfInstances), "UTF-8"));
        char delimiter = CsvConverter.determineDelimiter(table.getPath());
        int keyCol = 0;
        BufferedWriter writeTable = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(tableOut), "UTF-8"));
        List<String> instances = new ArrayList<String>();
        String line = readInstances.readLine();
        while (line != null) {
            instances.add(line.toLowerCase());
            line = readInstances.readLine();
        }
        line = readTable.readLine();
        int count = 0;
        while (line != null) {
            //add dbpedia labels
            if (count == 0) {
                String[] entries = line.split("" + delimiter + "");
                String[] newEntries = new String[entries.length];
                for (int i = 0; i < newEntries.length; i++) {
                    if (i == newEntries.length - 1) {
                        if (entries[i].startsWith("dbpedia_")) {
                            newEntries[i] = entries[i];
                        } else {
                            newEntries[i] = "dbpedia_" + entries[i];
                        }
                        writeTable.append(newEntries[i]);
                    } else {
                        if (entries[i].startsWith("dbpedia_")) {
                            newEntries[i] = entries[i];
                        } else {
                            newEntries[i] = "dbpedia_" + entries[i];
                        }
                        writeTable.append(newEntries[i] + "" + delimiter + "");
                    }
                }
                count++;
                writeTable.append("\n");
                line = readTable.readLine();
                continue;
            }
            String[] entries = line.split("" + delimiter + "");
            //System.out.println(entries[keyCol]);            
            String withoutCompany = "";
            String[] name = entries[keyCol].split(" ");
            for (int i = 0; i < name.length - 1; i++) {
                withoutCompany += name[i];
            }

            if (instances.contains(entries[keyCol].toLowerCase()) || instances.contains(withoutCompany.toLowerCase())) {
                writeTable.append(line + "\n");
                count++;
            }
            count++;
            line = readTable.readLine();
        }
        writeTable.flush();
        writeTable.close();
        return tableOut;
    }
}
