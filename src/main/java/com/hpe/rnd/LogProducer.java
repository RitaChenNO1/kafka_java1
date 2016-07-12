package com.hpe.rnd;

import net.sf.json.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.util.Properties;

/**
 * Created by chzhenzh on 7/8/2016.
 */
public class LogProducer {

    public static void main(String[] args) {
        //LogProducer<String, String> inner;
        Properties kafkaProperties = new Properties();
        Producer<String, String> producer =null;
        Properties topicProperties = new Properties();
        Properties UDPSocketProperties = new Properties();
        try {

            //1. receive data from UDP socket
            UDPSocketProperties.load(ClassLoader.getSystemResourceAsStream("UDPSocket.properties"));
            String host = UDPSocketProperties.getProperty("host");
            int port =Integer.parseInt(UDPSocketProperties.getProperty("port"));
            int bufferSize =Integer.parseInt(UDPSocketProperties.getProperty("bufferSize"));
            DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName(host));

            //2. properties setting for producer
            kafkaProperties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));
            //3. get the known topics
            topicProperties.load(ClassLoader.getSystemResourceAsStream("topics.properties"));
            String topicsKeywords[]=topicProperties.getProperty("topicsKeywords").split(",");
            String topics[]=topicProperties.getProperty("topics").split(",");
            String topicsStartKey=topicProperties.getProperty("topicsStartKey");
            String messageOtherTopics=topicProperties.getProperty("messageOtherTopics");
            String nonJsonTopics=topicProperties.getProperty("nonJsonTopics");
            String nonMessageTopics=topicProperties.getProperty("nonMessageTopics");
            int topicsNo=topicsKeywords.length<=topics.length?topicsKeywords.length:topics.length;
            //4. create the producer
            producer=new KafkaProducer<String, String>(kafkaProperties);
            DatagramPacket packet=null;
            byte[] buffer=null;
            while(true){
                //4.1 receive the UDP info
                buffer= new byte[bufferSize];
                packet= new DatagramPacket(buffer, buffer.length);
                socket.receive(packet);
                String str_packet=new String(packet.getData());
                //the info is json, get the keyword of key: message
                //eg: check the message: github github_production, then send to github_production topic
                if(str_packet.indexOf("{")>0 && str_packet.indexOf("}")>0) {
                    JSONObject jsonObject = JSONObject.fromObject(str_packet);
                    if (jsonObject.has(topicsStartKey)) {
                        String message = jsonObject.getString(topicsStartKey);
                        int i = 0;
                        for (; i < topicsNo; i++) {
                            if (message.indexOf(topicsKeywords[i]) > 0) {
                                producer.send(new ProducerRecord<String, String>(topics[i], str_packet));
                                break;
                            }
                        }
                        //not covered message topics
                        if (i == topicsNo) {
                            producer.send(new ProducerRecord<String, String>(messageOtherTopics, str_packet));
                        }
                    }else{
                        //not message start topics
                        producer.send(new ProducerRecord<String, String>(nonMessageTopics, str_packet));
                    }
                }else{
                    //plain text topic,not json
                    producer.send(new ProducerRecord<String, String>(nonJsonTopics, str_packet));
                }
                buffer=null;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
