package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.tools.ant.taskdefs.GZip;

import de.mannheim.uni.webtables.statistics.TableStatisticsExtractorLocal;

public class TableLookup {

	public static void main(String[] args) {

		File folder = new File("tmpTables");
		if (folder.canRead() == false) {
			folder.mkdir();
		}
		
		if(args[0].equals("simple"))
		{
			copyTables(readTablesFromFileSimple(args[1]), folder);
		}
		else if(args[0].equals("extract"))
		{
			try {
				File f = TableStatisticsExtractorLocal.extractTarFile(args[1]);
				System.out.println(f.getAbsolutePath());
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		else
		{
			try {

				// System.out.println(folder.getAbsolutePath());
				copyTables(readTablesFromFile(args[0], Integer.parseInt(args[1])),
						folder);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public static void copyTables(Map<String, List<String>> tablesMap,
			File outputFolder) {
		for (Entry<String, List<String>> entry : tablesMap.entrySet()) {
			if (entry.getKey().contains("tables_gz/tables")) {
				for (String fStr : entry.getValue()) {
					try {

						File f = new File(entry.getKey() + "/" + fStr);
						org.apache.commons.io.FileUtils.copyFile(f,
								new File(outputFolder.getAbsolutePath() + "/"
										+ f.getName()));
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			} else {
				File folder = null;
				try {
					System.out.println(entry.getKey());
					if(entry.getKey().endsWith("gz"))
						//folder = extractTarFileGzip(entry.getKey());
						folder = TableStatisticsExtractorLocal.extractTarFile(entry.getKey());
					else
						folder = extractTarFile(entry.getKey());
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				Set<String> missing = new HashSet<String>();
				for(String s : entry.getValue())
					missing.add(s);
				
				if (folder != null) {
					File[] files = folder.listFiles();
					for (File f : files) {
						for (String fStr : entry.getValue())
							if (f.getName().equals(fStr)) {
								try {
									org.apache.commons.io.FileUtils.copyFile(
											f,
											new File(outputFolder
													.getAbsolutePath()
													+ "/"
													+ f.getName()));
									missing.remove(fStr);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
							}
					}

					for(String s : missing)
						System.out.println("Missing file: " + s);
					
					try {
						org.apache.commons.io.FileUtils.deleteDirectory(folder);
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}

				}
			}
		}

	}

	public static Map<String, List<String>> readTablesFromFileSimple(String path) {
		try
		{
			BufferedReader r = new BufferedReader(new FileReader(path));
			Map<String, List<String>> results = new HashMap<String, List<String>>();
			
			
			String line = null;
			
			while((line = r.readLine()) != null)
			{
				String tarFile = null;
				
				/*line = line.replace("/html_tables/tar/",
						"/htmlTablesTarCompleted/");
				String tarFile = line.substring(0,
						line.indexOf("tar.gz") + 6) + ".tar";
				String tableName = line
						.substring(line.indexOf("tar.gz") + 7);*/
				
				tarFile = line.substring(0, line.lastIndexOf('/')) + ".tar.gz";
				
				String tableName = line.substring(line.lastIndexOf('/')+1);
				
				List<String> files = null;
				
				if(results.containsKey(tarFile))
					files = results.get(tarFile);
				else
				{
					files = new LinkedList<String>();
					results.put(tarFile, files);
				}
				
				files.add(tableName);
			}
			
			r.close();
			
			return results;
		}
		catch(Exception e)
		{
			e.printStackTrace();
		}
		
		return null;
	}
	
	public static Map<String, List<String>> readTablesFromFile(String path,
			int nmTables) {
		String startStr = "INFO: -----------------------------------------------/";

		Map<String, List<String>> tables = new HashMap<String, List<String>>();

		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(path));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			// skip the header
			String line = br.readLine();

			while ((line = br.readLine()) != null && nmTables > 0) {
				if (line.startsWith(startStr)) {
					nmTables--;
					line = line.replace(
							startStr.substring(0, startStr.length() - 1), "");
					line = line.replace(
							"-----------------------------------------------",
							"");
					String tarFile = "";
					String tableName = "";
					try {
						if (line.contains("/../../tables_gz/tables/")) {
							tarFile = line.substring(0,
									line.indexOf("tables/") + 6);
							tableName = line
									.substring(line.indexOf("tables/") + 7);
						} else {
							line = line.replace("/html_tables/tar/",
									"/htmlTablesTarCompleted/");
							tarFile = line.substring(0,
									line.indexOf("tar.gz") + 6) + ".tar";
							tableName = line
									.substring(line.indexOf("tar.gz") + 7);
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
					List<String> tablesList = new ArrayList<String>();
					if (tables.containsKey(tarFile)) {
						tablesList = tables.get(tarFile);

					}
					tablesList.add(tableName);
					tables.put(tarFile, tablesList);
					// tablesList.add(fileName);

				}
			}
		} catch (Exception e) {

		}

		return tables;
	}

	public static File extractTarFileGzip(String path) throws Exception {
		/* Read TAR File into TarArchiveInputStream */
		TarArchiveInputStream myTarFile = new TarArchiveInputStream(
				new GZIPInputStream(new FileInputStream(new File(path))));

		String outputDirName = path.replaceAll(".tar.gz$", "");
		File outputDir = new File(outputDirName);
		outputDir.mkdir();
		/* To read individual TAR file */
		TarArchiveEntry entry = null;
		String individualFiles;
		int offset;
		FileOutputStream outputFile = null;
		/* Create a loop to read every single entry in TAR file */
		while ((entry = myTarFile.getNextTarEntry()) != null) {
			/* Get the name of the file */
			individualFiles = entry.getName();
			/* Get Size of the file and create a byte array for the size */
			byte[] content = new byte[(int) entry.getSize()];
			offset = 0;
			/* Some SOP statements to check progress */
			// System.out.println("File Name in TAR File is: "
			// + individualFiles);
			// System.out.println("Size of the File is: " +
			// entry.getSize());
			// System.out.println("Byte Array length: " + content.length);
			/* Read file from the archive into byte array */
			myTarFile.read(content, offset, content.length - offset);
			/* Define OutputStream for writing the file */
			outputFile = new FileOutputStream(new File(outputDirName + "/"
					+ individualFiles));
			/* Use IOUtiles to write content of byte array to physical file */
			IOUtils.write(content, outputFile);
			/* Close Output Stream */
			outputFile.close();
		}
		/* Close TarAchiveInputStream */
		myTarFile.close();
		return outputDir;
	}
	
	public static File extractTarFile(String path) throws Exception {
		/* Read TAR File into TarArchiveInputStream */
		TarArchiveInputStream myTarFile = new TarArchiveInputStream(
				new FileInputStream(new File(path)));

		String outputDirName = path.replaceAll(".tar$", "");
		File outputDir = new File(outputDirName);
		outputDir.mkdir();
		/* To read individual TAR file */
		TarArchiveEntry entry = null;
		String individualFiles;
		int offset;
		FileOutputStream outputFile = null;
		/* Create a loop to read every single entry in TAR file */
		while ((entry = myTarFile.getNextTarEntry()) != null) {
			/* Get the name of the file */
			individualFiles = entry.getName();
			/* Get Size of the file and create a byte array for the size */
			byte[] content = new byte[(int) entry.getSize()];
			offset = 0;
			/* Some SOP statements to check progress */
			// System.out.println("File Name in TAR File is: "
			// + individualFiles);
			// System.out.println("Size of the File is: " +
			// entry.getSize());
			// System.out.println("Byte Array length: " + content.length);
			/* Read file from the archive into byte array */
			myTarFile.read(content, offset, content.length - offset);
			/* Define OutputStream for writing the file */
			outputFile = new FileOutputStream(new File(outputDirName + "/"
					+ individualFiles));
			/* Use IOUtiles to write content of byte array to physical file */
			IOUtils.write(content, outputFile);
			/* Close Output Stream */
			outputFile.close();
		}
		/* Close TarAchiveInputStream */
		myTarFile.close();
		return outputDir;
	}
}
