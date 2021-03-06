package com.kafkaTransformation.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

public class ConfigCache {
	final static Logger LOGGER = Logger.getLogger(ConfigCache.class);

	private final Properties configProp = new Properties();

	private static ConfigCache cache = null;

	private ConfigCache() {
		LOGGER.info("reading configuration.properties file");
		this.getClass().getClassLoader();
//		InputStream stream = ClassLoader.getSystemResourceAsStream("config.properties");
		InputStream stream;   

		try {
			stream = new FileInputStream("config.properties");
			configProp.load(stream);
		} catch (IOException e) {
			LOGGER.info("file not found :-" + e);
		}
	}

	public static ConfigCache getInstance() {
		if (cache == null) {
			cache = new ConfigCache();
			return cache;
		}
		return cache;
	}

	public String getProperty(String key) {
		return configProp.getProperty(key);
	}

	public boolean containsKey(String key) {
		return configProp.contains(key);
	}

}
