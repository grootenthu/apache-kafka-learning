package com.kafka.consumerdemo.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaConsumerAssignAndSeekDemo {
	
	public static void main(String[] args) {
		final Logger logger = LoggerFactory.getLogger(KafkaConsumerDemo.class);
		String bootstrapServers = "localhost:9092";
		String groupId = "my-java-app";
		boolean keepReading = true;
		int startMsgIndex = 0, endMsgIndex = 5;
		long offsetToReadFrom = 15L;

		// create producer properties
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		// create consumer
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

		//assign topic partition
		TopicPartition topicPartition = new TopicPartition("first_topic", 0);
		consumer.assign(Arrays.asList(topicPartition));
		
		//seek
		consumer.seek(topicPartition, offsetToReadFrom);
		
		// poll for new data
		while (keepReading) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

			for (ConsumerRecord<String, String> record : records) {
				startMsgIndex++;
				logger.info("Recieved record" + "\n");
				logger.info("Key : " + record.key() + " , Value : " + record.value() + " , Partition : "
						+ record.partition());
				if (startMsgIndex >= endMsgIndex) {
					keepReading = false;
					break;
				}
				
			}
		}
		logger.info("Finished replaying messages");
		consumer.close();
	
	}

}
