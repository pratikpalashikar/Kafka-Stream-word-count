package com.techmisal.streams;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class WordProducer {



    public static void main(String args[]) {

        Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count-producer");
        config.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        config.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String,String> producer  = new KafkaProducer<>(config);


        for(int i=0;i<10;i++) {

            System.out.println(i);

            producer.send(new ProducerRecord<String, String>("word_count", "pratik", "pratik"));
        }


        producer.close();

    }


}
