package com.hpe.rnd;

import com.hpe.rnd.DAO.Log;
import com.hpe.rnd.DAO.Topic;
import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Properties;

import static java.lang.System.exit;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class LogProducer {

    public static void main(String[] args) {
        //LogProducer<String, String> inner;
        Properties kafkaProperties = new Properties();
        Producer<String, String> producer = null;
        //Properties topicProperties = new Properties();
        Properties UDPSocketProperties = new Properties();
        try {
            //1. receive data from UDP socket
            UDPSocketProperties.load(ClassLoader.getSystemResourceAsStream("UDPSocket.properties"));
            String host = UDPSocketProperties.getProperty("host");
            int port = Integer.parseInt(UDPSocketProperties.getProperty("port"));
            int bufferSize = Integer.parseInt(UDPSocketProperties.getProperty("bufferSize"));
            DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName(host));
            //2. properties setting for producer
            kafkaProperties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));
            //3. get the known topics
            Topic topic = new Topic("topics.properties");
//            String topicsKeywords[] = topic.getTopicsKeywords();
//            String topics[] = topic.getTopics();
            String topicsStartKey = topic.getTopicsStartKey();
            String messageTopics = topic.getMessageTopics();
//            String messageOtherTopics = topic.getMessageOtherTopics();
            String nonJsonTopics = topic.getNonJsonTopics();
            String nonMessageTopics = topic.getNonMessageTopics();
//            int topicsNo = topic.getTopicsNo();
            //4. create the producer
            producer = new KafkaProducer<String, String>(kafkaProperties);
            DatagramPacket packet = null;
            byte[] buffer = null;
            while (true) {
                //4.1 receive the UDP info
                buffer = new byte[bufferSize];
                packet = new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String str_packet = new String(packet.getData());
                System.out.println(str_packet);

                //the info is json, get the keyword of key: message
                //eg: check the message: github github_production, then send to github_production topic
                if (str_packet.indexOf("{") >= 0 && str_packet.indexOf("}") >= 0) {
                    JSONObject jsonObject = JSONObject.fromObject(str_packet);
                    if (jsonObject.has(topicsStartKey)) {
                        producer.send(new ProducerRecord<String, String>(messageTopics, str_packet));
                    } else {
                        //not message start topics
                        producer.send(new ProducerRecord<String, String>(nonMessageTopics, str_packet));
                    }
                } else {
                    //plain text topic,not json
                    producer.send(new ProducerRecord<String, String>(nonJsonTopics, str_packet));
                }
                buffer = null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            Log.Info("Producer is closing...");
            producer.close();
        }
    }
}
