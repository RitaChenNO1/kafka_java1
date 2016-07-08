package com.hpe.rnd;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class LogConsumer{
    public static void main(String[] args) {
        Properties properties = new Properties();
        try {
            properties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
            consumer.subscribe(Arrays.asList("message", "test_git"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
