package com.example.kafka.pizza;

import net.datafaker.Faker;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;

public class PizzaProducer {
    public static final Logger logger = LoggerFactory.getLogger(PizzaProducer.class.getName());

    public static void sendPizzaMessage(KafkaProducer<String, String> kafkaProducer,
                                        String topicName,
                                        int iterCount,
                                        int interIntervalMillis,
                                        int intervalMillis,
                                        int intervalCount,
                                        boolean sync
    ) {
        int iterSeq = 0;
        long seed = 2022;
        long startTime = System.currentTimeMillis();

        PizzaMessage pizzaMessage = new PizzaMessage();
        Random random = new Random(seed);
        Faker faker = new Faker(random);

        while (iterSeq++ != iterCount) {
            HashMap<String, String> pMessage = pizzaMessage.produce_msg(faker, random, iterSeq);
            ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, pMessage.get("key"), pMessage.get("message"));
            sendMessage(kafkaProducer, producerRecord, pMessage, sync);

            if ((intervalCount > 0) && (iterSeq % intervalCount == 0)) {
                try {
                    logger.info("####### IntervalCount:" + intervalCount + " intervalMillis:" + intervalMillis + " #########");
                    Thread.sleep(intervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

            if (interIntervalMillis > 0) {
                try {
                    logger.info("interIntervalMillis:" + interIntervalMillis);
                    Thread.sleep(interIntervalMillis);
                } catch (InterruptedException e) {
                    logger.error(e.getMessage());
                }
            }

        }
        long endTime = System.currentTimeMillis();
        long timeElapsed = endTime - startTime;

        logger.info("{} millisecond elapsed for {} iterations", timeElapsed, iterCount);

    }

    public static void sendMessage(KafkaProducer<String, String> kafkaProducer,
                                   ProducerRecord<String, String> producerRecord,
                                   HashMap<String, String> pMessage,
                                   boolean sync
    ) {
        if (!sync) {
            kafkaProducer.send(producerRecord, (metadata, exception) -> {
                if (exception == null) {
                    logger.info("async message:" + pMessage.get("key") + " partition:" + metadata.partition() + " offset:" + metadata.offset());
                } else {
                    logger.error("exception error from broker " + exception.getMessage());
                }
            });
        } else {
            try {
                RecordMetadata metadata = kafkaProducer.send(producerRecord).get();
                logger.info("sync message:" + pMessage.get("key") + " partition:" + metadata.partition() + " offset:" + metadata.offset());
            } catch (ExecutionException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        }
    }

    public static void main(String[] args) {

        String topicName = "pizza-topic";

        //KafkaProducer configuration setting
        // null, "hello world"

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.101:9092");
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // acks 설정
        props.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        // batch.size 설정 (Record Accumulator 크기 설정.
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "32000");
        // linger 설정 (Record Accumulator 에서 배치를 기다리는 시간 20ms 이하 권장)
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");

        /**
         * 전송 / 재전송 설정 (delivery.timeout.ms >= linger.ms + request.timeout.ms)
         * max.block.ms -> Send() 호출 시 Record Accumulator 에 입력하지 못하고 block 되는 최대 시간 초과시 Exception 발생.
         *
         * linger.ms -> Sender Thread 가 Record Accumulator 에서 배치별로 가져가기 위한 최대시간
         * request.timeout.ms -> 전송에 걸리는 최대시간, 전송 재 시도 대기시간 제외, 초과시 retry 를 하거나 Timeout Exception 발생.
         * retry.timout.ms -> 전송 재 시도를 위한 대기시간.
         * delivery.timout.ms -> Producer 메시지 전송에 허용된 최대시간, 초과시 Timeout Exception 발생.
         */
        props.setProperty(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, "50000");

        /**
         * 중복없이 전송 설정 (kafka 3.0 부터 명시하지 않으면 기본 값이 true)
         * 명시하지 않았을경우 아래 설정값들이 바뀌면 기본값이 true로 되어 있어도 ENABLE_IDEMPOTENCE_CONFIG 설정이 적용되지 않음.
         *
         * 명시했을 경우 주의사항
         * acks 설정은 all
         * max_in_flight~ 설정은 1~5
         * retries 설정은 0 이상
         */
        props.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");

        //KafkaProducer object creation
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(props);

        sendPizzaMessage(kafkaProducer, topicName, -1, 1000, 0, 0, true);

        kafkaProducer.close();

    }
}