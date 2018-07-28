package com.techmisal.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Properties;

public class WordCount {


    public static void main(String[] args) {


        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG,"word-count");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        config.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG,Serdes.StringSerde.class);
        config.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG,Serdes.StringSerde.class);


        KStreamBuilder kStreamBuilder = new KStreamBuilder();


        KStream<String,String> stream = kStreamBuilder.stream("word_count");


        KTable<String,Long> kTable = stream.selectKey((key,val)->val).groupByKey().count();


        kTable.to(Serdes.String(),Serdes.Long(),"output-topic");


        KafkaStreams kafkaStreams = new KafkaStreams(kStreamBuilder,config);


        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        

    }


}
