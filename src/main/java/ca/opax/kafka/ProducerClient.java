package ca.opax.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.Producer;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;


import java.io.IOException;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.common.errors.TopicExistsException;

import ca.opax.kafka.model.DataRecord;

import java.util.Properties;
import java.util.Collections;
import java.util.Optional;

public class ProducerClient {

	public static void main(final String[] args) throws IOException {

		final Properties props = loadConfig();

		// Create topic if needed
		final String topic = "sakib1";
		createTopic(topic, props);


		Producer<String, DataRecord> producer = new KafkaProducer<String, DataRecord>(props);

		// Produce sample data
		final Long numMessages = 10L;
		for (Long i = 0L; i < numMessages; i++) {
			String key = "sakib";
			DataRecord record = new DataRecord(i, RandomStringUtils.randomAlphabetic(10));

			System.out.printf("Producing record: %s\t%s%n", key, record);
			producer.send(new ProducerRecord<String, DataRecord>(topic, key, record), new Callback() {
				@Override
				public void onCompletion(RecordMetadata m, Exception e) {
					if (e != null) {
						e.printStackTrace();
					} else {
						System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
					}
				}
			});
		}

		producer.flush();

		System.out.printf("10 messages were produced to topic %s%n", topic);

		producer.close();
	}

	public static Properties loadConfig() {
		final Properties props = new Properties();
		props.put("bootstrap.servers", "pkc-6ojv2.us-west4.gcp.confluent.cloud:9092");
		props.put("security.protocol", "SASL_SSL");
		props.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='W3RFATIDCLS4BDX2' password='eF4r+ZdRNyk4CATqu1+UZe+SHH9oo8F3mMnZ+hbXfatzrBC9mnC3Q9taOtW3Ynk0';");
		props.put("sasl.mechanism", "PLAIN");
		props.put("client.dns.lookup", "use_all_dns_ips");
		props.put("session.timeout.ms", "45000");
		props.put("acks", "all");
		props.put("schema.registry.url", "https://{{ SR_ENDPOINT }}");
		props.put("basic.auth.credentials.source", "USER_INFO");
		props.put("basic.auth.user.info", "W3RFATIDCLS4BDX2:eF4r+ZdRNyk4CATqu1+UZe+SHH9oo8F3mMnZ+hbXfatzrBC9mnC3Q9taOtW3Ynk0");
		
		props.put(ProducerConfig.ACKS_CONFIG, "all");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaJsonSerializer");

		return props;
	}
	
	// Create topic in Confluent Cloud
	public static void createTopic(final String topic, final Properties cloudConfig) {
		final NewTopic newTopic = new NewTopic(topic, Optional.empty(), Optional.empty());
		try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
			adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
		} catch (final InterruptedException | ExecutionException e) {
			// Ignore if TopicExistsException, which may be valid if topic exists
			if (!(e.getCause() instanceof TopicExistsException)) {
				throw new RuntimeException(e);
			}
		}
	}

}
