package de.mannheim.uni.infogather.preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.Vector.Element;
import org.apache.mahout.math.VectorWritable;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;

import com.sun.tools.hat.internal.parser.Reader;

import de.mannheim.uni.IO.ConvertFileToTable;
import de.mannheim.uni.TableProcessor.TableKeyIdentifier;
import de.mannheim.uni.hadoop.SequenceFileReader;
import de.mannheim.uni.hadoop.TextSequenceFileReader;
import de.mannheim.uni.index.SimpleLuceneIndex;
import de.mannheim.uni.model.Pair;
import de.mannheim.uni.model.Quad;
import de.mannheim.uni.model.Table;
import de.mannheim.uni.model.TableColumn;
import de.mannheim.uni.model.Triple;
import de.mannheim.uni.pipelines.Pipeline;
import de.mannheim.uni.schemamatching.label.StringNormalizer;
import de.mannheim.uni.utils.concurrent.Parallel;
import de.mannheim.uni.utils.concurrent.Producer;
import de.mannheim.uni.utils.concurrent.SynchronizedTextWriter;
import de.mannheim.uni.utils.concurrent.Parallel.Consumer;
import de.mannheim.uni.webtables.CompressedMETAIndexReader;
import de.mannheim.uni.webtables.ExtractedMETAReader;
import de.mannheim.uni.webtables.TableMetaData;

public class PreProcessor {

	/*
	 * 
	 * TODO
	 * 
	 * iterate over inverted index of the following features: context, table, url, context2table
	 * 
	 * generate pairs and get tf-idf score from petar's code
	 * 
	 * 
	 */
	
	private Pipeline pipe;
	private String output;
	
	public PreProcessor(Pipeline pipeline, String outputDir)
	{
		pipe = pipeline;
		output = outputDir;
	}
	
	public void createTableIndices(final List<String> inputFiles) throws IOException
	{
		System.out.println("Creating indices ...");
		// create indices
		final SimpleLuceneIndex colIdx = new SimpleLuceneIndex();
		final SimpleLuceneIndex tblIdx = new SimpleLuceneIndex();
		final SimpleLuceneIndex rowIdx = new SimpleLuceneIndex();
		final SimpleLuceneIndex attIdx = new SimpleLuceneIndex();
		
		
		colIdx.beginWrite(new File(output, "colIndex").getAbsolutePath());
		tblIdx.beginWrite(new File(output, "tblIndex").getAbsolutePath());
		rowIdx.beginWrite(new File(output, "rowIndex").getAbsolutePath());
		attIdx.beginWrite(new File(output, "attIndex").getAbsolutePath());

		// process input files
		System.out.println("Processing input files ...");
		new Parallel<Pair<Long, String>>().producerConsumer(new Producer<Pair<Long,String>>() {
			
			@Override
			public void execute() {
				for(String file : inputFiles)
				{
					try {
						SequenceFileTableReader reader = new SequenceFileTableReader(file, pipe);
						
						Pair<Long, String> value = null;
						
						while((value = reader.read()) != null)
						{
							produce(value);
						}
						
						reader.close();

					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					
				}
			}
		}, new Consumer<Pair<Long,String>>() {

			public void execute(Pair<Long, String> parameter) {
				de.mannheim.uni.IO.TableReader tr = new de.mannheim.uni.IO.TableReader(pipe);
				
				String key = Integer.toString(parameter.getFirst().intValue());

				try {
					Table t = tr.readTableFromString("", parameter.getSecond());
					
					if(t!=null && t.isHasKey() && t.getFirstKey()!=null)
					{
						// fill row index & table index
						StringBuilder table = new StringBuilder();
						// attributes use equal weights, so we only add each distinct term once
						TreeSet<String> attributes = new TreeSet<String>();
						// tuples use equal weights, so we only add each distinct term once
						TreeSet<String> tuple = new TreeSet<String>();
						
						
						StringBuilder keys = new StringBuilder();
						for(Integer row : t.getFirstKey().getValues().keySet())
						{
							keys.append(t.getFirstKey().getValues().get(row));
							keys.append(" ");
						}
						
						int colIndex=0;
						for(TableColumn c : t.getColumns())
						{
							if(c!=t.getFirstKey())
							{
								for(Integer row : t.getFirstKey().getValues().keySet())
								{
									if(c.getValues().containsKey(row))
									{
										tuple.add(t.getFirstKey().getValues().get(row).replace(" ", "")
												+ c.getValues().get(row).replace(" ", ""));
										table.append(c.getValues().get(row));
										table.append(" ");
									}
								}
								
								StringBuilder tupleDoc = new StringBuilder();
								for(String s : tuple)
									tupleDoc.append(s + " ");
								tuple.clear();
								rowIdx.addDocument(key + "\t" + colIndex, tupleDoc.toString());
								
								tblIdx.addDocument(key + "\t" + colIndex, keys.toString() + table.toString());
								
								for(String s : c.getHeader().split(" "))
									attributes.add(s);
								for(String s : t.getFirstKey().getHeader().split(" "))
									attributes.add(s);
								StringBuilder attDoc = new StringBuilder();
								for(String s : attributes)
									attDoc.append(s + " ");
								attributes.clear();
								attIdx.addDocument(key + "\t" + colIndex, attDoc.toString());
							}
							

							// fill column index
							StringBuilder col = new StringBuilder();
							TreeSet<String> values = new TreeSet<String>(c.getValues().values());
							
							for(String entry : values)
								col.append(entry + " ");
							colIdx.addDocument(key + "\t" + colIndex++, col.toString());
							
							colIndex++;
						}
						
					}
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		});
		
		System.out.println("Closing indices ...");
		// close indices
		colIdx.endWrite();
		tblIdx.endWrite();
		rowIdx.endWrite();
		attIdx.endWrite();
	}
	
	public void createHtmlIndices(final List<String> inputFiles, final String url_index, final String file_index) throws IOException
	{
		// create indices
		final SimpleLuceneIndex ctxIdx = new SimpleLuceneIndex();
		final SimpleLuceneIndex urlIdx = new SimpleLuceneIndex();
		
		ctxIdx.beginWrite(new File(output, "ctxIndex").getAbsolutePath());
		urlIdx.beginWrite(new File(output, "urlIndex").getAbsolutePath());

		final Map<String, Integer> fileIdx;
		try {
			System.out.println("Reading " + file_index + " ...");
			fileIdx = CompressedMETAIndexReader.readMappingAsMap(file_index);
		} catch (NumberFormatException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
			return;
		}
		
		// process input files
		// id, meta-list, url, html
		new Parallel<Quad<Long, List<TableMetaData>, String, String>>().producerConsumer(new Producer<Quad<Long, List<TableMetaData>, String, String>>() {
			
			@Override
			public void execute() {
				Map<Integer, String> urlKeys;
				try {
					System.out.println("Reading " + url_index + " ...");
					urlKeys = CompressedMETAIndexReader.readMappingInvertedAsMap(url_index);
				} catch (NumberFormatException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return;
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
					return;
				}
				
				for(String file : inputFiles)
				{
					try {
						SequenceFileReader<LongWritable, Text> r = new SequenceFileReader<LongWritable, Text>(file, LongWritable.class, Text.class);

						String metaFile = file.replace("_HTML_compressed", "_META");
						//System.out.println("Reading " + metaFile + " ...");
						List<TableMetaData> meta = ExtractedMETAReader.readAll(metaFile);
						
						while(r.next())
						{			
							String key = Long.toString(r.getKey().get());
							
							String url = urlKeys.get((int)r.getKey().get());
							//add URL to url-index here, so it is only added once!
							if(!url.isEmpty())
								urlIdx.addDocument(key, url);

							Quad<Long, List<TableMetaData>, String, String> t = new Quad<Long, List<TableMetaData>, String, String>(r.getKey().get(), meta, url, r.getValue().toString());

							produce(t);
						}
						
						r.close();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		}, new Consumer<Quad<Long, List<TableMetaData>, String, String>>() {

			public void execute(Quad<Long, List<TableMetaData>, String, String> parameter) {

				String key = parameter.getFirst().toString();
				
				try {
					// fill context index
					List<TableMetaData> metas = parameter.getSecond();
					String url = parameter.getThird();
					
					// get all TableMetaData objects that we have for the current URL
					for(TableMetaData meta : metas)
						if(meta.getUrl().equals(url))
							ctxIdx.addDocument(key + "\t" + fileIdx. get(meta.getFile()), HtmlContextExtractor.extractContext(parameter.getFourth(), meta.getTableStart(), meta.getTableEnd()));
						
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			}
		});
		
		// close indices
		ctxIdx.endWrite();
		urlIdx.endWrite();
	}
	
	public void flattenSimilarities(String path, String index) throws Exception
	{
		final SequenceFileReader<IntWritable, VectorWritable> input = new SequenceFileReader<IntWritable, VectorWritable>(path, IntWritable.class, VectorWritable.class);
		String out = new File(new File(path).getParent(), new File(path).getName() + "_flattened").getAbsolutePath();
		//BufferedWriter writer = new BufferedWriter(new FileWriter(out));
		final SynchronizedTextWriter writer = new SynchronizedTextWriter(out);
		final HashMap<Integer, String> idx = new HashMap<Integer, String>();
		SequenceFileReader<IntWritable, Text> indexReader = new SequenceFileReader<IntWritable, Text>(index, IntWritable.class, Text.class);
		
		System.out.println("Reading document index " + index + " ...");
		
		while(indexReader.next())
			idx.put(indexReader.getKey().get(), indexReader.getValue().toString());
		indexReader.close();
		
		System.out.println("Processing similarities ...");
		System.out.println("Writing to " + out);
		
		new Parallel<Pair<String, Vector>>().producerConsumer(new Producer<Pair<String,Vector>>() {
			
			@Override
			public void execute() {
				try {
					while(input.next())
					{
						String key = idx.get(input.getKey().get());
						Vector v = input.getValue().get();
						
						produce(new Pair<String, Vector>(key, v));
						
					}
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}, new Consumer<Pair<String,Vector>>() {

			public void execute(Pair<String, Vector> parameter) {
				String key = parameter.getFirst();
				Vector v = parameter.getSecond();
				
				
				for(Element e : v.nonZeroes())
				{
					double value = e.get();
					
					if(value>0.0)
					{
						String key2 = idx.get(e.index());
						writer.write(key + "\t" + key2 + "\t" + value);
					}
				}
			}
		});
		
	
		input.close();
		writer.flushAndBlock();
		
		
		System.out.println("Done");
	}
	
	public static void main(String[] args) throws Exception {
		/* set up command line parameters */
		Options options = new Options();
		
		/* parse command line parameters */
		CommandLineParser parser = new BasicParser();
		CommandLine cmd;
		
		Pipeline pipe = new Pipeline("test", "");
		pipe.setSilent(true);
		PreProcessor p = new PreProcessor(pipe, "preprocessing_output/");
		
		Date start = new Date(); 
		
		try {
			cmd = parser.parse(options, args);
			
			if(cmd.getArgs()[0].equals("createTableIndices") && cmd.getArgs().length>=2)
			{
				List<String> inputFiles = cmd.getArgList();
				inputFiles.remove(0);
				p.createTableIndices(inputFiles);
			}
			else if(cmd.getArgs()[0].equals("createHtmlIndices") && cmd.getArgs().length>=2)
			{
				List<String> inputFiles = cmd.getArgList();
				inputFiles.remove(0);
				String url_index = inputFiles.remove(0);
				String file_index = inputFiles.remove(0);
				p.createHtmlIndices(inputFiles, url_index, file_index);
			}
			else if(cmd.getArgs()[0].equals("flatten") && cmd.getArgs().length>=2)
			{
				List<String> inputFiles = cmd.getArgList();
				inputFiles.remove(0);
				String index = inputFiles.remove(0);
				
				
				for(String file : inputFiles)
					p.flattenSimilarities(file, index);
			}
				
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		Date end = new Date();
		
		System.out.println("Start: " + start.toString() + " - end: " + end.toString());
	}
}
