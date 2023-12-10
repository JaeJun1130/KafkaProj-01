package com.example.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerASyncCustomCB {
    private static final Logger logger = LoggerFactory.getLogger(ProducerASyncCustomCB.class.getName());

    public static void main(String[] args) {
        String topicName = "multipart-topic";

        // KafkaProducer Configuration setting
        Properties properties = new Properties();
        // bootstrap-servers, key.serializer.class, value.serializer.class
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        /**
         * 1. kafkaProducer 객체 생성
         * 2. ProducerRecord 객체 생성
         * 3. KafkaProducer message send
         *
         * Kafka Producer 전송은 Producer Client의 별도 Thread가 전송을 담당한다는 점에서 기본적으로 Thread간 Async 전송임.
         */
        try (KafkaProducer<Integer, String> kafkaProducer = new KafkaProducer<>(properties)) {
            for (int seq = 0; seq < 20; seq++) {
                ProducerRecord<Integer, String> producerRecord = new ProducerRecord<>(topicName, seq, "hello world" + seq);
                kafkaProducer.send(producerRecord,new CustomCallback(seq));
            }
        }
    }
}
