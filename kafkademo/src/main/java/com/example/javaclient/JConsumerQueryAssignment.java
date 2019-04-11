/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.KafkaProperties;

/**
 * 自动commit
 * Created by cuilei05 on 16/3/15.
 */
public class JConsumerQueryAssignment extends BaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JConsumerQueryAssignment.class);

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Arrays.asList(KafkaProperties.TOPIC_NAME));
        Set<TopicPartition> assignment = consumer.assignment();
        LOGGER.info("assignment:" + assignment.toString());

        final int minBatchSize = 200;
        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                assignment = consumer.assignment();
                LOGGER.info("assignment:{}, assign:{}", assignment.toString(), consumer.subscription());
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    handle(buffer);
                    consumer.commitSync();
                    buffer.clear();
                }
                Thread.sleep(500L);
            }
        } finally {
            consumer.close();
        }
    }
}
