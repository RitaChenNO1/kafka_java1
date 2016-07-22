package com.hpe.rnd;

import com.hpe.rnd.DAO.Log;
import com.hpe.rnd.DAO.Topic;
import com.hpe.rnd.DAO.Vertica;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class LogConsumer{
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        KafkaConsumer<String, String> consumer =null;
        Vertica vertica=null;
        try {
            //1. consumer properties
            kafkaProperties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
            consumer= new KafkaConsumer<String, String>(kafkaProperties);
            //2. topic properties
            Topic topic=new Topic("topics.properties");
            String topicsKeywords[]=topic.getTopicsKeywords();
            String topicsStartKey=topic.getTopicsStartKey();
            String messageOtherTopics=topic.getMessageOtherTopics();
            String topicsString=topic.getTopicsString();
            int topicsNo=topic.getTopicsNo();
            String topicsVerticaTables[]=topic.getTopicsVerticaTables();
            String topicsAll[]=(topicsString+","+messageOtherTopics).split(",");

            String verticaTableColumnLength=topic.getVerticaTableColumnLength();
            consumer.subscribe(Arrays.asList(topicsAll));
            Log.Info("Listening Topic:" + Arrays.toString(topicsAll));
            //3. vertica connection
            vertica=new Vertica("vertica.properties");
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);
                //System.out.println("records NO:"+records.count());
                for (ConsumerRecord<String, String> record : records) {
                   // System.out.println("in100");
                    //related topic, sent to related vertica table
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    String str_packet = record.value();
                    if (str_packet.indexOf("{") >= 0 && str_packet.indexOf("}") >= 0) {
                        System.out.println("in1");
                        JSONObject jsonObject = JSONObject.fromObject(str_packet);
                        if (jsonObject.has(topicsStartKey)) {
                            String message = jsonObject.getString(topicsStartKey);
                            int i;
                            for (i=0; i < topicsNo; i++) {
                                if (message.indexOf(topicsKeywords[i]) > 0) {
                                    //System.out.println(topicsKeywords[i]);
                                    //get the table columns, and values
                                    //remove @ for the column name
                                    //Object jsonKeys[] = jsonObject.keySet().toArray();
                                    Object jsonKeys[]=jsonObject.keySet().toString().replace("[", "").replace("]", "").replace("@","").split(",");
                                    Object jsonValues[] = jsonObject.values().toArray();
                                    int columnNo = jsonKeys.length;
                                    String tableName = topicsVerticaTables[i];
                                    vertica.intoVertica(tableName, jsonKeys, jsonValues, verticaTableColumnLength, columnNo);
                                    break;
                                }
                            }
                            //not covered message topics
                            if (i == topicsNo) {
                                System.out.println("not covered now!!!!!");
                            }
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            System.out.println("close");
            Log.Info("Vertica connection is closing...");
            vertica.closeConn();
            Log.Info("Consumer is closing...");
            consumer.close();
        }


    }
}
