package com.example.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomCallback implements Callback {
    private static final Logger logger = LoggerFactory.getLogger(CustomCallback.class.getName());

    private int seq;

    public CustomCallback(int seq) {
        this.seq = seq;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception == null) {
            logger.info("seq: {}, partition: {}, offset: {}, timeStamp: {}, metadata: {}", this.seq, metadata.partition(), metadata.offset(), metadata.offset(), metadata.toString());
            return;
        }

        logger.error("producer send exception:", exception);
    }
}
