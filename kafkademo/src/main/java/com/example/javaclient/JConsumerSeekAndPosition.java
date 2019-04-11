/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.KafkaProperties;

/**
 * 演示seek的作用
 * Created by cuilei05 on 16/3/15.
 */
public class JConsumerSeekAndPosition extends BaseConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JConsumerSeekAndPosition.class);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test1");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer consumer = new KafkaConsumer<>(props);

        String topic = KafkaProperties.TOPIC_NAME;

        TopicPartition partition0 = new TopicPartition(topic, 0);
        TopicPartition partition1 = new TopicPartition(topic, 1);

        // 订阅特定分区
        consumer.assign(Arrays.asList(partition0, partition1));
        LOGGER.info("assign**************");
        printNextOffset(consumer, partition0, partition1);

        // 跳到开始
        LOGGER.info("seekToBeginning**************");
        consumer.seekToBeginning(Arrays.asList(partition0, partition1));
        printNextOffset(consumer, partition0, partition1);

        //跳到结束
        LOGGER.info("seekToEnd**************");
        consumer.seekToEnd(Arrays.asList(partition0, partition1));
        printNextOffset(consumer, partition0, partition1);

        LOGGER.info("seek to 10 and 15**************");
        consumer.seek(partition0, 10);
        consumer.seek(partition1, 15);
        printNextOffset(consumer, partition0, partition1);

        consumer.close();
    }

    /**
     * 打印consumer在分区的将要消费的下一个消息的offset
     *
     * @param consumer
     * @param partitions
     */
    private static void printNextOffset(KafkaConsumer consumer, TopicPartition... partitions) {
        for (TopicPartition partition : partitions) {
            LOGGER.info("next offset for {}:{}", partition, consumer.position(partition));
        }
    }

}
