package com.gis.consumer.raw;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.*;

public class BatchApp {
    //default values
    private static String brokers = "localhost:9092";
    private static String groupId = "gis-batch-raw";
        private static String topic = "gis-uk-crime-demo";
    private static String outputDir="/home/jrp/GisBatchAppOutput";

    private static KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    public static void main(String[] args) {
        if (args.length == 4) {
            brokers = args[0];
            groupId = args[1];
            topic = args[2];
            outputDir = args[3];
        }

        runConsumer();
    }

    private static void runConsumer() {
        System.out.println("Running Batch Consumer");
        //creating consumer
        KafkaConsumer<String, String> consumer = createConsumer();

        //subscribing to topic
        consumer.subscribe(Collections.singletonList(topic));

        final int minBatchSize = 05;
        List<String> buffer = new ArrayList<>();
        try {
            while (true) {
                final ConsumerRecords<String, String> consumerRecords = consumer
                        .poll(1000);
                consumerRecords.forEach(record -> {
                    buffer.add(record.value());
                    System.out.printf("Consumer Record:(%s, %s, %d, %d)\n",
                            record.key(), record.value(), record.partition(),
                            record.offset());
                });

                if (buffer.size() >= minBatchSize) {
                    store(buffer);
                    consumer.commitSync();
                    buffer.clear();
                    break;
                }
            }
        } finally {
            consumer.close();
        }
    }

    private static void store(List<String> buffer) {
        System.out.println("Storing data");
        long startTime = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setMaster("local[*]")
                .setAppName("BatchApp")
                .set("spark.executor.instances", "2");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> batchRDD = jsc.parallelize(buffer);

        batchRDD.saveAsTextFile("file://"
                + outputDir + "/kafkabkp_"
                + startTime);
        jsc.close();
    }
}
