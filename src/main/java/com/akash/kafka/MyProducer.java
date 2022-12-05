package com.akash.kafka;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Properties;

public class MyProducer {
    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.ERROR);
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer-id1"); // producer unique-id
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093"); // producer unique-id
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName()); // serialize the values(bytes) as they are sent over network
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<Integer,String> producer = new KafkaProducer<Integer, String>(props); //key is integer and message is string

        producer.send(new ProducerRecord<Integer,String>("greeting",1,"This is first message"));
        //every message has a timestamp, default is create time when producer produces it.
        //if we specify property message.timestamp.type = 1 then timestamp is set to time at which broker receives the record.
        //records are serialized (bytes) to be sent over to network(broker).
        //same key message will go to same partition help in join,grouping using a partitioner.
        //message is sent to partition buffer by default based on system hash logic.
        //then together all messages in the various partition buffers are sent to broker to optimize the network instead of sending 1 by 1 message to broker partitions.
        //default buffer size is 32mb.
        //IO thread is used to send message to broker cluster.
        //if the buffer is full, send method has to wait and after certain time it will give exception.(can happen if IO thread is slow).
        //by default kafka has atleast once semantics, wherein if IO thread doesn't receive acknowledgment from cluster, it will retry to take message from producer atleast once.

        producer.close();

    }
}
