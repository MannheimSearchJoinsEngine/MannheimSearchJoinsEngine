package de.mannheim.uni.mongodb;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.net.UnknownHostException;
import java.util.Set;
import java.util.zip.GZIPOutputStream;

import com.mongodb.DB;
import com.mongodb.MongoClient;

public class Test {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		// try {
		// MongoClient mongoClient = new MongoClient("localhost", 27017);
		// DB db = mongoClient.getDB("loddb");
		// Set<String> colls = db.getCollectionNames();
		//
		// for (String s : colls) {
		// System.out.println(s);
		// }
		// } catch (UnknownHostException e) {
		// // TODO Auto-generated catch block
		// e.printStackTrace();
		// }

		// writeStringtoGZ("dsadsadd\n", "here.gz");
		StringBuilder sb = new StringBuilder();
		sb.append("sdad" + "\n");
		sb.append("sadsad" + "\n");
		System.out.println(sb.toString());
	}

	public static void writeStringtoGZ(String value, String path) {
		FileOutputStream output = null;
		try {
			output = new FileOutputStream(path);
			Writer writer = new OutputStreamWriter(
					new GZIPOutputStream(output), "UTF-8");
			try {
				writer.write(value);
			} finally {
				writer.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				output.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

}
