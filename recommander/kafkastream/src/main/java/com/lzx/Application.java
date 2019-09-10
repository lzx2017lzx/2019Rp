package com.lzx;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;
import java.util.stream.Stream;

public class Application {
    public static void main(String[] args) {
        String brokers = "192.168.0.111:9092";
        String zookeeper = "192.168.0.111:2181";
        String fromTopic = "logone";
        String toTopic = "recom";

        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zookeeper);

        StreamsConfig config = new StreamsConfig(settings);

        TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("SOURCE", fromTopic)
                .addProcessor("PROCESS", ()->new LogProcessor(),"SOURCE")
                .addSink("SINK", toTopic, "PROCESS");

        KafkaStreams streams = new KafkaStreams(builder,config);
        streams.start();


    }

}

