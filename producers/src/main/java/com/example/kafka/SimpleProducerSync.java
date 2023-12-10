package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SimpleProducerSync {
    private static final Logger logger = LoggerFactory.getLogger(SimpleProducerSync.class.getName());

    public static void main(String[] args) {
        String topicName = "simple-topic";

        // KafkaProducer Configuration setting
        Properties properties = new Properties();
        // bootstrap-servers, key.serializer.class, value.serializer.class
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 1. kafkaProducer 객체 생성
         * 2. ProducerRecord 객체 생성
         * 3. KafkaProducer message send
         *
         * Kafka Producer 전송은 Producer Client의 별도 Thread가 전송을 담당한다는 점에서 기본적으로 Thread간 Async 전송임.
         */
        try (KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties)) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, "hello world 3");
            RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();

            logger.info("\n ######## record meta data received #### \n" +
                    "partition:" + recordMetadata.partition() + "\n" +
                    "offset:" + recordMetadata.offset() + "\n" +
                    "timestamp" + recordMetadata.timestamp());

        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
    }
}
