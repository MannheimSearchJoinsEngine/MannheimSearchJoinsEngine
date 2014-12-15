package de.mannheim.uni.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

/**
 * @author petar
 * 
 */
public class FileUtils {

	public static double getFileSize(File file) {

		double bytes = file.length();
		double kilobytes = (bytes / 1024);
		double megabytes = (kilobytes / 1024);
		return megabytes;

	}

	public static List<String> readFilesFromFolder(String folderPath,
			double maxSize) {
		List<String> files = new ArrayList<String>();
		File folder = new File(folderPath);

		for (File fileEntry : folder.listFiles()) {
			// System.out.println("added" + fileEntry.getAbsolutePath());
			if (FileUtils.getFileSize(fileEntry) <= maxSize
					&& (fileEntry.getName().endsWith(".gz") || fileEntry
							.getName().endsWith(".tar")))

				files.add(fileEntry.getAbsolutePath());
		}
		return files;
	}

	/**
	 * reads the allowed table when searching by column header
	 * 
	 * @return
	 */
	public static TreeSet<String> readHeaderTablesFromFile(String filePath) {
		TreeSet<String> tablesList = new TreeSet<String>();
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(filePath));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		try {
			String line = "";

			while ((line = br.readLine()) != null) {

				try {
					if (line.contains("|")) {
						String fileName = line.split("\"\\|\"")[2].replace(
								"\"", "");

						tablesList.add(fileName);

					}

				} catch (Exception e) {

				}
			}
		} catch (Exception e) {

		}
		return tablesList;

	}

	/**
	 * reads the data files paths for InfoGather
	 * 
	 * @param folderPath
	 * @return
	 */
	public static Map<String, List<String>> getFilePaths(String folderPath) {
		Map<String, List<String>> paths = new HashMap<String, List<String>>();

		File folder = new File(folderPath);
		for (File fileEntry : folder.listFiles()) {
			String fileName = fileEntry.getName();
			if (fileName.endsWith("CSV")) {
				String filePath = fileEntry.getAbsolutePath();
				String prexis = fileName.replace("_CSV", "");
				List<String> innerPaths = new LinkedList<String>();
				if (paths.containsKey(prexis)) {
					innerPaths = paths.get(prexis);
				}
				innerPaths.add(0, filePath);
				paths.put(prexis, innerPaths);
			} else if (fileName.endsWith("HTML")) {
				String filePath = fileEntry.getAbsolutePath();
				String prexis = fileName.replace("_HTML", "");
				List<String> innerPaths = new LinkedList<String>();
				if (paths.containsKey(prexis)) {
					innerPaths = paths.get(prexis);
				}
				if (innerPaths.size() < 1)
					innerPaths.add(0, "dummy");
				innerPaths.add(1, filePath);
				paths.put(prexis, innerPaths);
			} else if (fileName.endsWith("META")) {
				String filePath = fileEntry.getAbsolutePath();
				String prexis = fileName.replace("_META", "");
				List<String> innerPaths = new LinkedList<String>();
				if (paths.containsKey(prexis)) {
					innerPaths = paths.get(prexis);
				}
				if (innerPaths.size() < 2) {
					innerPaths.add(0, "dummy");
					innerPaths.add(1, "dummy");
				}
				innerPaths.add(2, filePath);
				paths.put(prexis, innerPaths);
			}

		}

		return paths;
	}
}
