package com.github.lfrezarini.kafka.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.metrics.stats.Count;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThread {
    private static final String BOOTSTRAP_SERVER = "127.0.0.1:9092";
    private static final String TOPIC = "first_topic";
    private static final String GROUP_ID = "my_fourth_application";

    private Logger logger;
    private CountDownLatch consumerThreadLatch;
    private ConsumerRunnable consumerRunnable;

    public static void main(String[] args) {
        new ConsumerDemoWithThread().run();
    }

    private ConsumerDemoWithThread() {
        logger = LoggerFactory.getLogger(ConsumerDemoWithThread.class.getName());
        consumerThreadLatch = new CountDownLatch(1);
    }

    private void run() {
        initializeConsumerThread();
        addShutdownHook();
        awaitConsumerExecutionToEnd();
    }

    private void initializeConsumerThread() {
        logger.info("Creating the consumer thread");
        consumerRunnable = new ConsumerRunnable(BOOTSTRAP_SERVER, GROUP_ID, TOPIC, consumerThreadLatch);
        Thread myThread = new Thread(consumerRunnable);
        myThread.start();
    }

    private void addShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            consumerRunnable.shutdown();

            try {
                consumerThreadLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                logger.info("Application has exited.");
            }
        }));
    }

    private void awaitConsumerExecutionToEnd() {
        try {
            consumerThreadLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application got interrupted", e);
        } finally {
            logger.info("Application is closing");
        }
    }


    public class ConsumerRunnable implements Runnable {

        private String bootstrapServer;
        private String groupId;
        private String topic;
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerRunnable(String bootstrapServer, String groupId, String topic, CountDownLatch latch) {
            this.bootstrapServer = bootstrapServer;
            this.groupId = groupId;
            this.topic = topic;
            this.latch = latch;

            initializeConfigs();
        }

        private void initializeConfigs() {
            logger = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
            consumer = new KafkaConsumer<String, String>(getKafkaProducerProperties());
            consumer.subscribe(Collections.singleton(topic));
        }

        private Properties getKafkaProducerProperties() {
            Properties properties = new Properties();

            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);

            return properties;
        }

        @Override
        public void run() {
            try {
                runConsumerListener();
            } catch (WakeupException e) {
                handleWakeupException();
            } finally {
                closeConsumerAndLatchCountdown();
            }

        }

        private void runConsumerListener() {
            while (true) {
                listenToIncomingRecords();
            }
        }

        private void handleWakeupException() {
            logger.info("Received shutdown signal!");
        }

        private void closeConsumerAndLatchCountdown() {
            consumer.close();
            // tell the main code that the consumer is dead
            latch.countDown();
        }

        private void listenToIncomingRecords() {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            readRecords(records);
        }

        private void readRecords(ConsumerRecords<String, String> records) {
            for (ConsumerRecord record : records) {
                logRecord(record);
            }
        }

        private void logRecord(ConsumerRecord record) {
            logger.info("Key: {}, Value: {}", record.key(), record.value());
            logger.info("Partition: {}, Offset: {}", record.partition(), record.offset());
        }

        private void shutdown() {
            // Special method that interrupt consumer.poll()
            // Throws a WakeUpException
            consumer.wakeup();
        }
    }
}
