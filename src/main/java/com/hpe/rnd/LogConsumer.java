package com.hpe.rnd;

import net.sf.json.JSONObject;
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
        Properties kafkaProperties = new Properties();
        Properties topicProperties = new Properties();
        try {
            kafkaProperties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(kafkaProperties);
            topicProperties.load(ClassLoader.getSystemResourceAsStream("topics.properties"));
            String topics=topicProperties.getProperty("topics");
            String messageOtherTopics=topicProperties.getProperty("messageOtherTopics");
            String nonJsonTopics=topicProperties.getProperty("nonJsonTopics");
            String nonMessageTopics=topicProperties.getProperty("nonMessageTopics");
            String topicsAll[]=topics.concat(",").concat(messageOtherTopics).concat(",").concat(nonJsonTopics)
                    .concat(",").concat(nonMessageTopics).split(",");
            consumer.subscribe(Arrays.asList(topicsAll));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                for (ConsumerRecord<String, String> record : records)
                {
                    //related topic, sent to related vertica table
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    JSONObject jsonObject = JSONObject.fromObject(record.value());
                    Object jsonKeys[] = jsonObject.keySet().toArray();
                    Object jsonValues[]=jsonObject.values().toArray();
                    //System.out.println(jsonKeys[0]);
                    //System.out.println(jsonValues[0]);
                    System.out.println(jsonObject.keySet().toString());
            }
        }
        } catch (IOException e) {
            e.printStackTrace();
        }


    }
}
