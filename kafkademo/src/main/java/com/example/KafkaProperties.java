package com.example;

public interface KafkaProperties {
    final static String ZK_CONNECT = "127.0.0.1:2181";
    final static String GROUPID = "group1";
    final static String TOPIC_NAME = "my_topic";

    final static String BROKER_CONNECT = "127.0.0.1:9091,127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094";
}
