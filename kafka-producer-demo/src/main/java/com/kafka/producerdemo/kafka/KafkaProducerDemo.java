package com.kafka.producerdemo.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaProducerDemo {
	
	public static void main(String[] args) {
		String bootstrapServers = "localhost:9092";
		
		//create producer properties
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		//create the producer
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);
		for (int i=0; i<12; i++) {
			//create producer record
			ProducerRecord<String, String> producrRecord = new ProducerRecord<String, String>("first_topic", "hello world - " + i);
			//send data - asynchronously
			kafkaProducer.send(producrRecord);
			
			//flush data
			kafkaProducer.flush();
			
		}
		
		//flush producer and close
		kafkaProducer.close();
		
	}

}
