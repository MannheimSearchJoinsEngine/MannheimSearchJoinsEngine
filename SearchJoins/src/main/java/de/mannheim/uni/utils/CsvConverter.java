package de.mannheim.uni.utils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.zip.GZIPOutputStream;

import de.mannheim.uni.IO.ConvertFileToTable;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.File;

/**
 * Converts any CSV-File into the format required by SearchJoin and outputs the
 * gzipped version of the file.
 *
 * @author Oliver
 *
 */
public class CsvConverter {

    public static void main(String[] args) {
        if (args.length != 2) {
            System.out
                    .println("Converts an existing CSV-file into the format required by SearchJoin.");
            System.out.println("Usage: [input-file] [output-file]");
            return;
        }

        CsvConverter c = new CsvConverter();

        c.convertCsv(args[0], args[1]);
    }

    public void convertCsv(String input, String output) {
    	convertCsv(input, output, false);
    }
    
    public void convertCsv(String input, String output, boolean keepLists) {
        System.out.println("Converting " + input);

        CSVReader reader = null;
        CSVWriter writer = null;

        try {

            char delimiter = determineDelimiter(input);
            System.out.println("deli: " + delimiter);
            // create reader
            reader = new CSVReader(new InputStreamReader(new FileInputStream(
                    input), "UTF-8"), delimiter);
            // create writer
            writer = new CSVWriter(new OutputStreamWriter(new GZIPOutputStream(
                    new FileOutputStream(output)), "UTF-8"));

            // this array will hold the values for the current row
            String[] values;
            boolean header = true;
            // read all rows
            while ((values = reader.readNext()) != null) {
                if(header) {
                    writer.writeNext(values);
                    header = false;
                    continue;
                }
                String[] newValues = new String[values.length];
                // clean the string values for each column
                int i = 0;
                for (String v : values) {         
                    if (v.matches("^\\{.+\\|.+\\}$") && !keepLists) {
                        String firstListValue = v.split("\\{")[1];
                        firstListValue = firstListValue.split("\\|")[0];
                        newValues[i] = ConvertFileToTable.simpleStringNormalization(firstListValue, true);
                        i++;
                    } else {
                        newValues[i] = ConvertFileToTable.simpleStringNormalization(v, true);
                        i++;
                    }
                }
                // write the current row
                writer.writeNext(newValues);
            }

            // close reader and writer
            reader.close();
            writer.close();

            System.out.println("Done writing " + output);
        } catch (UnsupportedEncodingException e) {
            System.out.println("Unsupported Encoding!");
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            System.out.println("File not found!");
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static char determineDelimiter(String input) throws FileNotFoundException, UnsupportedEncodingException, IOException {
        File inputFile = new File(input);
        BufferedReader readTable = new BufferedReader(new InputStreamReader(new FileInputStream(inputFile), "UTF-8"));
        int tab = 0, semicolon = 0, comma = 0;
        String line = readTable.readLine();
        while (line != null) {
            if (line.contains("\t")) {
                tab++;
            }
            if (line.contains(";")) {
                semicolon++;
            }
            if (line.contains(",")) {
                comma++;
            }
            line = readTable.readLine();
        }
        if (tab > semicolon && tab > comma) {
            return '\t';
        }
        if (semicolon > tab && semicolon > comma) {
            return ';';
        }
        if (comma > semicolon & comma > tab) {
            return ',';
        }
        return ',';
    }
}
