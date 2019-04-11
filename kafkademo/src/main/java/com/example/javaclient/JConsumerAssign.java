/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import com.example.KafkaProperties;

/**
 * 订阅特定分区,不会负载均衡,
 * @author 71972
 */
public class JConsumerAssign extends BaseConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9082");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<>(props);

        String topic = KafkaProperties.TOPIC_NAME;

        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // 只订阅特定分区
        consumer.assign(Arrays.asList(partition0, partition1));

        // 从头开始消费
        consumer.seekToBeginning(Arrays.asList(partition0, partition1));

        //consumer.seekToBeginning(partition0);
        // consumer.seekToEnd(partition0);

        consumer.seek(partition0, 10);
        consumer.seek(partition1, 90);

        final int minBatchSize = 200;

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(120));
                for (ConsumerRecord<String, String> record : records) {
                    buffer.add(record);
                }
                if (buffer.size() >= minBatchSize) {
                    handle(buffer);
                    buffer.clear();
                }
            }
        } finally {
            consumer.close();
        }
    }

}
