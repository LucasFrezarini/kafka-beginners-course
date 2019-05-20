package com.github.lfrezarini.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";
    private static final String GROUP_ID = "my_fourth_application";
    private static Logger logger;
    private static KafkaConsumer<String, String> consumer;

    public static void main(String[] args) {
        initializeConfigs();
        subscribeConsumerToTopic();
        runConsumerListener();
    }

    private static void initializeConfigs() {
        logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
        consumer = new KafkaConsumer<String, String>(getKafkaProducerProperties());
    }

    private static void subscribeConsumerToTopic() {
        consumer.subscribe(Collections.singleton(TOPIC));
    }

    private static void runConsumerListener() {
        while (true) {
            listenToIncomingRecords();
        }
    }

    private static Properties getKafkaProducerProperties() {
        Properties properties = new Properties();

        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);

        return properties;
    }

    private static void listenToIncomingRecords() {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
        readRecords(records);
    }

    private static void readRecords(ConsumerRecords<String, String> records) {
        for (ConsumerRecord record : records) {
            logRecord(record);
        }
    }

    private static void logRecord(ConsumerRecord record) {
        logger.info("Key: {}, Value: {}", record.key(), record.value());
        logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
    }

}
