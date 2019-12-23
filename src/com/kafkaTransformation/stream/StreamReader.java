
package com.kafkaTransformation.stream;

import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaTransformation.util.ConfigCache;
import com.kafkaTransformation.util.KTConstants;

public class StreamReader {

	final static Logger LOGGER = Logger.getLogger(StreamReader.class);

	public static Properties getConfig() throws FileNotFoundException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.CONSUMER_ID_CONFIG));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.CONSUMER_BOOTSTRAP_SERVERS));
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return props;
	}

	public static StreamsBuilder getStream() throws FileNotFoundException {
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder
				.stream(ConfigCache.getInstance().getProperty(KTConstants.kAFKA_INPUT_TOPIC));

		textLines.map(
				(KeyValueMapper<? super String, ? super String, ? extends KeyValue<? extends Object, ? extends Object>>) (
						k, v) -> {
					System.out.println(v.toString());
							HashMap<String, String> kv = new HashMap<String, String>();
					final Pattern p = Pattern.compile("args=.*\"");
					Matcher m;
					m = p.matcher(v);
					while (m.find()) {
						String key_value[] = m.group(0).replace("args=\"", "").replace("\"", "").split("&");
						for (int i = 0; i < key_value.length; i++) {
							String key = key_value[i].split("=")[0];
							String value = key_value[i].split("=")[1];
							kv.put(key, value);
						}
					}
					String json = null;
					try {
						json = new ObjectMapper().writeValueAsString(kv);
					} catch (JsonProcessingException e) {

						e.printStackTrace();
					}
					KeyValue<Object, String> keyValue = new KeyValue<>(null, json);

					return keyValue;
				});

		textLines.to(ConfigCache.getInstance().getProperty(KTConstants.KAFKA_OUTPUT_TOPIC));

		/*
		 * KTable<String, Long> wordCounts = textLines .flatMapValues(textLine ->
		 * Arrays.asList(textLine.toLowerCase().split("\\W+"))) .groupBy((key, word) ->
		 * word) .count(Materialized.<String, Long, KeyValueStore<Bytes,
		 * byte[]>>as("counts-store"));
		 * 
		 * 
		 * wordCounts.toStream().to(ConfigCache.getInstance().getProperty(KTConstants.
		 * KAFKA_OUTPUT_TOPIC), Produced.with(Serdes.String(), Serdes.Long()));
		 * 
		 */
		return builder;
	}

	public void streamBuilder() {
		LOGGER.info("indside streamBuilder");
		KafkaStreams streams = null;
		try {
			streams = new KafkaStreams(StreamReader.getStream().build(), StreamReader.getConfig());
			streams.start();
		} catch (Exception e) {
			LOGGER.info("there is exeception in stream" + e);
			streams.close();
		}

	}

	public static void main(String[] args) {
		StreamReader reader = new StreamReader();
		reader.streamBuilder();
	}
}