package com.kafkaTransformation.stream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;

import com.kafkaTransformation.util.ConfigCache;
import com.kafkaTransformation.util.KTConstants;

public class StreamReader {

	final static Logger LOGGER = Logger.getLogger(StreamReader.class);

	public static Properties getConfig() throws FileNotFoundException {
		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.KAFKA_GROUP_ID));
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.BOOTSTRAP_SERVERS_CONFIG));
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		return props;
	}

	public static StreamsBuilder getStream() throws FileNotFoundException {
		StreamsBuilder builder = new StreamsBuilder();
		KStream<String, String> textLines = builder
				.stream(ConfigCache.getInstance().getProperty(KTConstants.kAFKA_INPUT_TOPIC));
		
		KTable<String, Long> wordCounts = textLines
				.flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\\W+")))
				.groupBy((key, word) -> word)
				.count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts-store"));
		
		
		wordCounts.toStream().to(ConfigCache.getInstance().getProperty(KTConstants.KAFKA_OUTPUT_TOPIC),
				Produced.with(Serdes.String(), Serdes.Long()));
		
		
		LOGGER.info("inside getStream");
		return builder;
	}

	public void streamBuilder() {
		LOGGER.info("indside streamBuilder");
		KafkaStreams streams = null;
			try {
		streams = new KafkaStreams(StreamReader.getStream().build(), StreamReader.getConfig());
		streams.start();
		}
		catch(Exception e) {
			LOGGER.info("there is exeception in stream"+e);
		streams.close();
		}
		
	}
	public static void main(String[] args) {
		StreamReader reader = new StreamReader();
		reader.streamBuilder();
	}
}
