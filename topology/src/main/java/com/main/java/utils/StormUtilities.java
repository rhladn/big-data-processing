package com.main.java.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.storm.kafka.spout.FirstPollOffsetStrategy;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;

import java.util.UUID;

@Slf4j
public class StormUtilities {

    /**
     * Creates a {@link KafkaSpout} using the configurations provided.
     */
    public static KafkaSpout getKafkaSpoutInstance(String topic) {
        String hosts = ""; // provide cluster hosts ip seperated by ,
        String id = topic + UUID.randomUUID().toString();

        String valueDeserializer = StringDeserializer.class.getName();

        KafkaSpout kafkaSpout = new KafkaSpout<>(
                KafkaSpoutConfig
                        .builder(hosts, topic)
                        .setFirstPollOffsetStrategy(FirstPollOffsetStrategy.LATEST)
                        .setProp(ConsumerConfig.GROUP_ID_CONFIG, id)
                        .setProp(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName())
                        .setProp(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer)
                        .build()
        );
        return kafkaSpout;
    }
}