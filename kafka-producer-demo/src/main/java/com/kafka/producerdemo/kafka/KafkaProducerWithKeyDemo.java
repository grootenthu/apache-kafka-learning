package com.kafka.producerdemo.kafka;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaProducerWithKeyDemo {
	
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		final Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallbackDemo.class);
		String bootstrapServers = "localhost:9092";
		
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
		for (int i=0; i<12; i++) {
			String topic = "first_topic";
			String message = "Hello World ";
			String key = "id_" + i;
			logger.info("Sending key {}", key);
			//create producer record
			ProducerRecord<String, String> producrRecord = new ProducerRecord<String, String>(topic, key, message + i);
			//send data - asynchronously
			kafkaProducer.send(producrRecord, new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception == null) {
						logger.info("Recieved Callback.... " + "\n" +
								"Topic Name : " + metadata.topic() + "\n" +
								"Partition : " + metadata.partition() + "\n" + 
								"Offset : " + metadata.offset());
					} else {
						logger.error(exception.getMessage());
					}
				}
			});
			
			//flush data
			kafkaProducer.flush();
			
		}
		
		//flush producer and close
		kafkaProducer.close();
	}
}
