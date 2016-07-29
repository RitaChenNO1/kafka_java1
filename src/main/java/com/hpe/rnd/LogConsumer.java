package com.hpe.rnd;

import com.hpe.rnd.DAO.HDFS;
import com.hpe.rnd.DAO.Log;
import com.hpe.rnd.DAO.Topic;
import com.hpe.rnd.DAO.Vertica;
import net.sf.json.JSONObject;
import org.apache.hadoop.hdfs.util.ExactSizeInputStream;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static java.lang.System.exit;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class LogConsumer {
    public static void main(String[] args) {
        Properties kafkaProperties = new Properties();
        KafkaConsumer<String, String> consumer = null;
        Vertica vertica = null;
        HDFS whdfs = null;
        try {
            //1. consumer properties
            kafkaProperties.load(ClassLoader.getSystemResourceAsStream("consumer.properties"));
            consumer = new KafkaConsumer<String, String>(kafkaProperties);
            //2. topic properties
            Topic topic = new Topic("topics.properties");
//            String topicsKeywords[] = topic.getTopicsKeywords();
            String topicsStartKey = topic.getTopicsStartKey();
            String messageTopics = topic.getMessageTopics();
            String messageOtherTopics = topic.getMessageOtherTopics();
            String nonJsonTopics = topic.getNonJsonTopics();
//            String topicsString = topic.getTopicsString();
//            int topicsNo = topic.getTopicsNo();
//            String topicsVerticaTables[] = topic.getTopicsVerticaTables();
            String topicsAll[] = (messageTopics + "," + messageOtherTopics + "," + nonJsonTopics).split(",");


            String verticaTableColumnLength = topic.getVerticaTableColumnLength();
            consumer.subscribe(Arrays.asList(topicsAll));
//            Log.Info("Listening Topic:" + Arrays.toString(topicsAll));
            Log.Info("Listening Topic:" + Arrays.toString(topicsAll));
            //3. vertica connection
            vertica = new Vertica("vertica.properties");
            whdfs=new HDFS("HDFS.properties");
            String nonJson_hdfspath = new String("nonJson.txt");
            String megOther_hdfspath = new String("msgOther.txt");
            String msgErr_hdfspath = new String("msgErr.txt");

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(100);

                for (ConsumerRecord<String, String> record : records) {
                    boolean result = false;
                    //related topic, sent to related vertica table
                    System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
                    String str_packet = record.value();
                    JSONObject jsonObject = JSONObject.fromObject(str_packet);

                    try {

                        if (str_packet.indexOf("{") >= 0 && str_packet.indexOf("}") >= 0) {
                            System.out.println("in1");
                            if (jsonObject.has(topicsStartKey)) {
                                System.out.println("****************json object with message keyword**************");
                                Object jsonKeys[] = jsonObject.keySet().toString().replace("[", "").replace("]", "").replace("@", "").replace(" ", "").split(",");
                                Object jsonValues[] = jsonObject.values().toArray();
                                String tableName = getTableName(jsonObject, topicsStartKey);
                                System.out.println("*****************table name is : *******" + tableName);

                                Object kv[] = vertica.splitMessage(jsonKeys, jsonValues, tableName);

                                Object newJsonKeys[] = (Object[]) kv[0];
                                Object newJsonValues[] = (Object[]) kv[1];
                                System.out.println("****************************************");
                                System.out.println("newJsonKeys=" + Arrays.toString(newJsonKeys));
                                System.out.println("newJsonValues=" + Arrays.toString(newJsonValues));
                                System.out.println("****************************************");

                                int columnNo = newJsonKeys.length;

                                vertica.intoVertica(tableName, newJsonKeys, newJsonValues, verticaTableColumnLength, columnNo);

                            } else {
                                System.out.println("****************json object with out message keyword write to hdfs**************");
                                whdfs.writeHDFS(str_packet,megOther_hdfspath);
                            }

                        }else{
                            System.out.println("****************nonJson object write to hdfs**************");
                            whdfs.writeHDFS(str_packet,nonJson_hdfspath);
                        }

                        result = true;
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    if (!result) {
//                        write to hdfs
                        System.out.println("this message with keyword \"message\" but have error will be write to hdfs");
                        whdfs.writeHDFS(str_packet,msgErr_hdfspath);

                    }

                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            System.out.println("close");
            Log.Info("Vertica connection is closing...");
            vertica.closeConn();
            Log.Info("Consumer is closing...");
            consumer.close();
        }


    }

    //    define some useful funcitons
    public static String getTableName(JSONObject jsonObject, String topicsStartKey) {
        String tableName = null;
        if (jsonObject.has(topicsStartKey)) {
            String message = jsonObject.getString(topicsStartKey);
            String msg[] = message.split(" ");

            for (int k = 0; k < msg.length; k++) {
                if (msg[k].indexOf(":") >= 0 && !(msg[k].replace(":", "").matches("[0-9]{1,}"))) {
                    //Remove the special characters in the string
                    tableName = Pattern.compile("[^a-zA-Z0-9_]").matcher(msg[k]).replaceAll("").trim();
                    break;
                }
            }

        } else {
            System.out.println("the msg doesn't contain message!!!");
        }
        return tableName;
    }


}
