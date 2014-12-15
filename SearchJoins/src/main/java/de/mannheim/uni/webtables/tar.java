package de.mannheim.uni.webtables;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.xeustechnologies.jtar.TarEntry;
import org.xeustechnologies.jtar.TarOutputStream;

public class tar {
	public static void main(String[] args) throws Exception {
		// Output file stream
		FileOutputStream dest = new FileOutputStream(
				"C:/Users/petar/Desktop/test.tar");

		// Create a TarOutputStream
		TarOutputStream out = new TarOutputStream(
				new BufferedOutputStream(dest));
		// Files to tar
		File[] filesToTar = new File[2];
		filesToTar[0] = new File(
				"C:/Users/petar/Documents/ProjectsFiles/SearchJoins/WebTables/webExtraction2/common-crawl_parse-output_segment_1346823845675_1346864466526_10.arc.gz385451698153491164.tar.gz1321361086542536580.tar.gz");
		filesToTar[1] = new File(
				"C:/Users/petar/Documents/ProjectsFiles/SearchJoins/WebTables/webExtraction2/common-crawl_parse-output_segment_1346823845675_1346864469604_0.arc.gz5926280269610541101.tar.gz2140587242356307018.tar.gz");
		for (File f : filesToTar) {

			out.putNextEntry(new TarEntry(f, f.getName().replace(
					"common-crawl_parse-output_segment_", "")));
			BufferedInputStream origin = new BufferedInputStream(
					new FileInputStream(f));

			int count;
			byte data[] = new byte[2048];
			while ((count = origin.read(data)) != -1) {
				out.write(data, 0, count);
			}

			out.flush();
			origin.close();
		}

		out.close();
	}
}
