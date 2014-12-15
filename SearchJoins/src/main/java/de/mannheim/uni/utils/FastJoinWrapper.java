package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.ProcessBuilder.Redirect;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;

public class FastJoinWrapper {

     public final static String FASTJOIN_EXE_WIN = "fastjoin/win32/FastJoin.exe";
     public final static String FASTJOIN_MEASURE = "FJACCARD";
     public final static double FASTJOIN_DELTA     = 0.5;
     public final static double FASTJOIN_TAU     = 0.5;
     public final static String ONTO1             = "temp/onto1.txt";
     public final static String ONTO2               = "temp/onto2.txt";

     ArrayList<String> scoreEntity1Entity2;

     public static void main(String[] args){
         /*FastJoinWrapper fjw = new FastJoinWrapper();
         fjw.run();
         ArrayList<String>  results = fjw.getResults();
         for (String match : results) {
             System.out.println(match);
         }*/
    	 
    	 //FastJoinWrapper fjw = new FastJoinWrapper();
    	 //fjw.extractMatches(Double.parseDouble(args[0]), args[1]);
    	 
    	 FastJoinWrapper fj = new FastJoinWrapper();
    	 
    	 fj.match(args[0], args[1], Pipeline.getPipelineFromConfigFile("test", "searchjoins.conf"));
     }

     public ArrayList<String> getResults(){
         //this.run();
         return this.scoreEntity1Entity2;
     }
     
     public void extractMatches(double minConfidence, String inputFile)
     {
         try {
             
             BufferedReader bri = new BufferedReader(new FileReader(inputFile));

             String line;
             double confidence = 0.0;
             String entity1;
             String entity2;

             while ((line = bri.readLine()) != null) {
                 confidence = Double.parseDouble(line.split(" ")[0]);
                 entity1 = bri.readLine();
                 entity2 = bri.readLine();
                 bri.readLine();
                 // change here for you desired output format
                 if(confidence>=minConfidence)
                	 System.out.println(entity1);
             }
             
             bri.close();
         } catch (IOException e) {
             e.printStackTrace();
         }
     }

     public Set<String> match(Collection<String> input1, Collection<String> input2, final Pipeline pipeline)
     {
    	 String file1, file2;
    	 
    	 file1 = "found_keys.txt";
    	 file2 = "query_keys.txt";
    	 
		final ConcurrentLinkedQueue<String> normalisedKeys = new ConcurrentLinkedQueue<String>();
		

		for(String value : input1)
		{
			value = StringNormalizer.clearString4FastJoin(value, true, pipeline.isSearchStemmedKeys());
			
			normalisedKeys.add(value);
		}
		
		try {
			BufferedWriter keyWriter = new BufferedWriter(new FileWriter(file1));
			
			for(String s : normalisedKeys)
				keyWriter.write(s + "\n");
			
			keyWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		HashSet<String> qKeys = new HashSet<String>();
		try
		{
			BufferedWriter queryKeys = new BufferedWriter(new FileWriter(file2));
			for(String key : input2)
			{
				String value = StringNormalizer.clearString(key, false, pipeline.isSearchStemmedKeys());
				queryKeys.write(value + "\n");
				qKeys.add(value);
			}
			queryKeys.close();
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		Set<String> result = match(file1, file2, pipeline);
		
		new File(file1).delete();
		new File(file2).delete();
		
		return result;
     }
     
     public Set<String> match(String input1, String input2, Pipeline pipeline){
         Process p;
         List<String> cmd = new ArrayList<String>();
         cmd.add(pipeline.getFastjoinPath());
         cmd.add(pipeline.getFastjoinMeasure());
         cmd.add(Double.toString(pipeline.getFastjoinDelta()));
         cmd.add(Double.toString(pipeline.getFastjoinTau()));
         cmd.add(input1);
         cmd.add(input2);
         ProcessBuilder pb = new ProcessBuilder(cmd);
         try {
             System.out.println("Running ...");
             for(String s : cmd)
            	 System.out.print(s + " ");
             System.out.println();
             scoreEntity1Entity2 = new ArrayList<String>();
             //p = Runtime.getRuntime().exec(pipeline.getFastjoinPath() + " " + pipeline.getFastjoinMeasure() + " " + pipeline.getFastjoinDelta() + " " + pipeline.getFastjoinTau() + " " + input1 + " " + input2);
             p = pb.start();
             
             BufferedReader bri = new BufferedReader(new InputStreamReader(p.getInputStream()));
             BufferedWriter w = new BufferedWriter(new FileWriter("fastjoin.output"));
             
            
             String line;
             double confidence = 0.0;
             String entity1;
             String entity2;
             
             HashSet<String> matched = new HashSet<String>();

             while ((line = bri.readLine()) != null) {
                 confidence = Double.parseDouble(line.split(" ")[0]);
                 entity1 = bri.readLine();
                 entity2 = bri.readLine();
                 bri.readLine();
                 // change here for you desired output format
                 //scoreEntity1Entity2.add(confidence + ": " + entity1 + " = " + entity2);
                 if(confidence>=pipeline.getFastjoinMinConf())
                	 matched.add(entity1);
                 
                 w.write("(" + entity1 + ", " + entity2 + ", " + confidence + ")\n");
             }
             bri.close();
             w.close();
             
             BufferedReader er = new BufferedReader(new InputStreamReader(p.getErrorStream()));
             while((line = er.readLine()) != null) {
        		//System.err.println(line);
             }
             er.close();
             
             //System.out.println("FastJoin ended with exit code " + p.exitValue());
             
             return matched;
         } catch (IOException e) {
             e.printStackTrace();
         }

         return null;
     }
}
