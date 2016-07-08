package com.hpe.rnd;

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
        Properties properties = new Properties();
        Producer<String, String> producer =null;
        try {
            //1. receive data from UDP socket
            String host = "16.250.41.248";
            int port = 10000;
            DatagramSocket socket = new DatagramSocket(port, InetAddress.getByName(host));
            byte[] buf = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buf, buf.length);
            //2. properties setting for producer
            properties.load(ClassLoader.getSystemResourceAsStream("producer.properties"));
            /*properties.put("bootstrap.servers", "localhost:9092");  // kafka address */
            //3. create the producer
            producer=new KafkaProducer<String, String>(properties);
            while(true){
                socket.receive(packet);
                producer.send(new ProducerRecord<String, String>("test_git",new String(packet.getData())));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            producer.close();
        }
    }
}
