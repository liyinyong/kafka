/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 手动按分区commit
 * Created by cuilei05 on 16/3/15.
 */
public class JConsumerManualCommit1 extends BaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JConsumerManualCommit1.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("foo", "bar"));
        final int minBatchSize = 200;

        List<ConsumerRecord<String, String>> buffer = new ArrayList<>();
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);

                // 挨个分区处理数据
                for (TopicPartition partition : records.partitions()) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> record : partitionRecords) {

                        // 模拟处理消息
                        System.out.println(record.offset() + ": " + record.value());
                    }

                    long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();

                    Map<TopicPartition, OffsetAndMetadata> map =
                            Collections.singletonMap(partition, new OffsetAndMetadata(lastOffset + 1));

                    // commit offset
                    consumer.commitSync(map);
                }
            }
        } finally {
            consumer.close();
        }
    }

}
