package com.kafkaTransformation.stream;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.databind.ObjectMapper;

public class TestFileReader {

	public static void main(String[] args) {
		BufferedReader reader;
		final Pattern p = Pattern.compile("args=.*\"");
		Matcher m;

		try {
			reader = new BufferedReader(new FileReader("D:\\\\dump\\\\nginx_logs_sample.txt"));
			String line = reader.readLine();

			while (line != null) {
				HashMap<String, String> kv = new HashMap<String, String>();
				m = p.matcher(line);
				while (m.find()) {
					String key_value[] = m.group(0).replace("args=\"", "").replace("", "").split("&");
					for (int i = 0; i < key_value.length; i++) {
						String key = key_value[i].split("=")[0];
						String value = key_value[i].split("=")[1];
						kv.put(key, value);
					}
				}
				String json = new ObjectMapper().writeValueAsString(kv);
				System.out.println(json);
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
