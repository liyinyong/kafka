/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.KafkaProperties;

/**
 * 自动commit
 * Created by cuilei05 on 16/3/15.
 */
public class JConsumerQueryMetaData extends BaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JConsumerQueryMetaData.class);

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
        Map<String, List<PartitionInfo>> topics = consumer.listTopics();
        LOGGER.info("topics:{}", topics);

        Map<MetricName, ? extends Metric> metrics = consumer.metrics();
        LOGGER.info("metrics:{}", topics);

        List<PartitionInfo> partitionsInfo = consumer.partitionsFor(KafkaProperties.TOPIC_NAME);
        LOGGER.info("partitionsInfo:{}", partitionsInfo);
    }
}
