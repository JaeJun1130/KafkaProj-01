package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Set;

public class SimpleConsumer {
    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class.getName());

    public static void main(String[] args) {
        String topicName = "simple-topic";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "simple-group");

        /**
         * Heart Beat Thread를 통해서 브로커의 Group Coordinator에 Consumer의 상태를 전송.
         * Heart Beat을 보내는 간격 sesstion.timeout.ms 보다 1/3 낮게 설정 권장.
         */
        properties.setProperty(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "5000");

        /**
         * 브로커가 Consumer로 Heart Beat을 기다리는 최대 시간. 해당 시간동안 받지 못하면 Consumer를 Group에서 제외 하도록 Rebalancing 명령을 지시.
         */
        properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "90000");

        /**
         * 이전 Poll() 호출 후 다음 Poll() 호출까지 브로커가 기다리는 시간. 해당 시간동안 Poll() 호출이 이뤄지지 않으면 해당 Consumers는 문제가 있는걸로 판단하여
         * Rebalanced 명령을 보냄.
         */
        properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "600000");


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Set.of(topicName));

        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(1000));
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                logger.info("record key: {}, value: {}, partition: {}", consumerRecord.key(), consumerRecord.value(), consumerRecord.partition());
            }
        }
//        kafkaConsumer.close();
    }
}
