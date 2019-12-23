package com.kafkaTransformation.stream;

import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafkaTransformation.util.ConfigCache;
import com.kafkaTransformation.util.KTConstants;

public class ContentKafkaStream {

	private static Consumer<Long, String> createConsumer() throws FileNotFoundException {
		final Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.CONSUMER_BOOTSTRAP_SERVERS));
		props.put(ConsumerConfig.GROUP_ID_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.CONSUMER_ID_CONFIG));
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		final Consumer<Long, String> consumer = new KafkaConsumer<>(props);

		consumer.subscribe(
				Collections.singletonList(ConfigCache.getInstance().getProperty(KTConstants.kAFKA_INPUT_TOPIC)));
		return consumer;
	}

	private static Producer<Long, String> createProducer() throws FileNotFoundException {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.DESTINATION_BOOTSTRAP_SERVERS));
		props.put(ProducerConfig.CLIENT_ID_CONFIG,
				ConfigCache.getInstance().getProperty(KTConstants.PRODUCER_ID_CONFIG));
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}

	static void runConsumer() throws InterruptedException, FileNotFoundException {
		final Consumer<Long, String> consumer = createConsumer();
		final Producer<Long, String> producer = createProducer();

		while (true) {
			final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);

			consumerRecords.forEach(record -> {
				HashMap<String, String> kv = new HashMap<String, String>();
				final Pattern p = Pattern.compile("args=.*\"");
				Matcher m;
				String line = record.value();
				m = p.matcher(line);
				while (m.find()) {
					String key_value[] = m.group(0).replace("args=\"", "").replace("\"", "").split("&");
					for (int i = 0; i < key_value.length; i++) {
						String key = key_value[i].split("=")[0];
						String value = key_value[i].split("=")[1];
						kv.put(key, value);
					}
				}
				String json = "";
				try {
					json = new ObjectMapper().writeValueAsString(kv);
				} catch (JsonProcessingException e) {

					e.printStackTrace();
				}
				ProducerRecord<Long, String> precord = new ProducerRecord<>(
						ConfigCache.getInstance().getProperty(KTConstants.KAFKA_OUTPUT_TOPIC), record.key(), json);

				try {
					RecordMetadata metadata = producer.send(precord).get();
				} catch (InterruptedException | ExecutionException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				System.out.printf("Consumer Record:(%d, %s, %d, %d)\n", record.key(), record.value(),
						record.partition(), record.offset());
			});

			consumer.commitAsync();
		}
	}

	public static void main(String[] args) throws InterruptedException, FileNotFoundException {
		ContentKafkaStream.runConsumer();
	}
}
