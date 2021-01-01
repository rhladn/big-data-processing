package com.main.java.utils;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Map;
import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

/**
 * Created by rahul.tandon
 */

// implementation of Kafka client used to push data to Kafka as key value pairs
@Slf4j
public class KafkaPublisher<K, V> implements Closeable {

    private final KafkaProducer<K, V> kafkaProducer;
    private final MetricRegistry metricRegistry;
    public static final String KAFKA_PUSH_LATENCY = "kafka.push.latency";

    public KafkaPublisher(Map<String, Object> kafkaConfig, com.main.java.utils.Metricregistry recoMetricRegistry) {
        if (kafkaConfig == null) {
            throw new RuntimeException("Kafka configs cannot be null");
        }
        Properties kafkaProperties = getPublisherProperties(kafkaConfig);
        kafkaProducer = new KafkaProducer<>(kafkaProperties);
        metricRegistry = recoMetricRegistry.getMetricRegistry();
        log.info("Instantiated KafkaPublisher with properties = {}", kafkaProperties);
    }

    /**
     * This method should be implemented in subclass to provide the Key Serializer class path
     */
    protected String getKeySerializer() {
        return StringSerializer.class.getName();
    };

    /**
     * This method should be implemented in subclass to provide the Value Serializer class path
     */
    protected String getValueSerializer() {
        return StringSerializer.class.getName();
    }

    /**
     * @return - Kafka Producer Properties
     */
    private Properties getPublisherProperties(Map<String, Object> kafkaConfig) {
        Properties properties = new Properties();
        if (kafkaConfig.get(BOOTSTRAP_SERVERS_CONFIG) == null) {
            throw new RuntimeException("Brokers not provided in config");
        }
        properties.put(BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.get(BOOTSTRAP_SERVERS_CONFIG));

            //Added default values below in-case if not provided in kafkaConfig map
        properties.put(BUFFER_MEMORY_CONFIG, kafkaConfig.getOrDefault(BUFFER_MEMORY_CONFIG, "33554432"));
        properties.put(BATCH_SIZE_CONFIG, kafkaConfig.getOrDefault(BATCH_SIZE_CONFIG, "16384"));
        properties.put(ACKS_CONFIG, kafkaConfig.getOrDefault(ACKS_CONFIG, "1"));
        properties.put(RETRIES_CONFIG, kafkaConfig.getOrDefault(RETRIES_CONFIG, "2"));
        properties.put(CLIENT_ID_CONFIG, kafkaConfig.getOrDefault(CLIENT_ID_CONFIG, "client"));
        properties.put(LINGER_MS_CONFIG, kafkaConfig.getOrDefault(LINGER_MS_CONFIG, "0"));
        properties.put(COMPRESSION_TYPE_CONFIG, kafkaConfig.getOrDefault(COMPRESSION_TYPE_CONFIG, "none"));
        properties.put(REQUEST_TIMEOUT_MS_CONFIG, kafkaConfig.getOrDefault(REQUEST_TIMEOUT_MS_CONFIG, "30000"));
        properties.put(RETRY_BACKOFF_MS_CONFIG, kafkaConfig.getOrDefault(RETRY_BACKOFF_MS_CONFIG, "100"));

        //provide implementation of required serializer in your child-class
        properties.put(KEY_SERIALIZER_CLASS_CONFIG, getKeySerializer());
        properties.put(VALUE_SERIALIZER_CLASS_CONFIG, getValueSerializer());
        return properties;
    }

    /**
     * @param record - ProducerRecord
     * @return - Exitcode.SUCCESS if data published successfully otherwise ExitCode.FAILURE
     */
    private ExitCode publish(ProducerRecord<K, V> record) {
        try (Timer.Context ignored = metricRegistry.timer(KAFKA_PUSH_LATENCY).time()) {
            kafkaProducer.send(record);
        } catch (Exception e) {
            log.error("Failed to send record to kafka topic {}", record.topic(), e);
            return ExitCode.FAILURE;
        }
        return ExitCode.SUCCESS;
    }

    /**
     * @param topic - topic name
     * @param value - value
     * @return - Exitcode.SUCCESS if data published successfully otherwise ExitCode.FAILURE
     */
    public ExitCode publish(String topic, V value) {
        ProducerRecord<K, V> data = new ProducerRecord<>(topic, value);
        return publish(data);
    }

    /**
     * @param topic - topic name
     * @param key   - key
     * @param value - value
     * @return - Exitcode.SUCCESS if data published successfully otherwise ExitCode.FAILURE
     */
    public ExitCode publish(String topic, K key, V value) {
        ProducerRecord<K, V> data = new ProducerRecord<>(topic, key, value);
        return publish(data);
    }

    /**
     * Closes this kafkaProducer client and releases any system resources associated with it.
     */
    @Override
    public void close() {
        kafkaProducer.close();
    }
}