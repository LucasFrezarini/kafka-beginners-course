package com.github.lfrezarini.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";
    private static Logger logger;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        initializeConfigs();
        sendNRecords(10, "hello world");
        disposeProducer();
    }

    private static void initializeConfigs() {
        logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        producer = new KafkaProducer<String, String>(getKafkaProducerProperties());
    }

    private static Properties getKafkaProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return properties;
    }

    private static void sendNRecords(int amount, String prefix) {
        for (int i = 0; i < amount; i++) {
            String key = "id_" + i;
            sendRecord(key, prefix + " " + i);
        }
    }

    private static void sendRecord(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, key, value);

        // Records with the same key will always goes to the same partition!
        logger.info("Key: " + key);

        producer.send(record, new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                handleRecordSendCompletion(recordMetadata, e);
            }
        });
    }

    private static void handleRecordSendCompletion(RecordMetadata recordMetadata, Exception e) {
        if (e == null) {
            logSendRecordMetadata(recordMetadata);
        } else {
            logSendRecordError(e);
        }
    }

    private static void logSendRecordMetadata(RecordMetadata recordMetadata) {
        logger.info("Received new metadata. \n" +
            "Topic: {}\n" +
            "Partition: {}\n" +
            "Offset: {}\n" +
            "Timestamp: {}\n", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
    }

    private static void logSendRecordError(Exception e) {
        logger.error("Error while producing", e);
    }

    private static void disposeProducer() {
        producer.flush();
        producer.close();
    }
}
