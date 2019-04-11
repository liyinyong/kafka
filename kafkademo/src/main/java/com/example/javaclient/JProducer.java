/*
 * Copyright (C) 2016 Baidu, Inc. All Rights Reserved.
 */
package com.example.javaclient;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.KafkaProperties;

/**
 * Created by cuilei05 on 16/3/15.
 */
public class JProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(JConsumerAutoCommit.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9082");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        // props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("partitioner.class", "com.example.javaclient.JPartitioner");
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        long start = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            ProducerRecord<Integer, String> record =
                    new ProducerRecord<Integer, String>(KafkaProperties.TOPIC_NAME, i, Integer.toString
                            (i));
            LOGGER.info("send:{}", record);

            Future<RecordMetadata> future = producer.send(record, callBack);

          /*  RecordMetadata recordMetadata = future.get();
            LOGGER.info("recordMetadata:{}",
                    ToStringBuilder.reflectionToString(recordMetadata, ToStringStyle.SHORT_PREFIX_STYLE));*/

        }
        long end = System.currentTimeMillis();
        LOGGER.info("Time used:{}", end - start);
        producer.close();
    }

    private static Callback callBack = new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            LOGGER.debug("onCompletion, xmetadata:{}, exception:{}", metadata, exception);
        }
    };
}
