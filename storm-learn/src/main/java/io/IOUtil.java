package io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class IOUtil {

	public static DateTimeFormatter getDateFormatter1() {
		return DateTimeFormat.forPattern("yyyy-MM-dd");
	}

	public static DateTimeFormatter getDateFormatter2() {
		return DateTimeFormat.forPattern("hh-mm-ss SSS");
	}

	public static File createFile(File delDir) throws IOException {
		DateTime time = DateTime.now();
		String txt = "result-" + time.toString(getDateFormatter2());
		File file = new File(delDir, txt + ".txt");
		file.createNewFile();
		return file;
	}

	public static void log(String content) throws IOException {
		DateTime time = DateTime.now();
		String dir = time.toString(getDateFormatter1());
		File delDir = new File(".\\" + dir);
		if (!delDir.exists()) {
			delDir.mkdirs();
		}
		FileWriter writer = new FileWriter(createFile(delDir));
		writer.write(content);
		writer.flush();
		writer.close();
	}

	public static String read(File file) throws IOException {
		FileReader reader = new FileReader(file);
		BufferedReader br = new BufferedReader(reader);
		StringBuilder builder = new StringBuilder();
		String s;
		while ((s = br.readLine()) != null) {
			builder.append(s);
		}
		br.close();
		return builder.toString();
	}

}
