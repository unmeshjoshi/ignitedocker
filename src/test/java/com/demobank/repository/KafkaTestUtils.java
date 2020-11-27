package com.demobank.repository;

import net.manub.embeddedkafka.EmbeddedKafka;
import net.manub.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;
import scala.collection.immutable.Map;

import java.util.HashMap;
import java.util.Properties;
import java.util.UUID;

public class KafkaTestUtils {

    public static void startEmbeddedKafka(String brokerListenAddress) {
        EmbeddedKafka.start(createKafkaConfig(brokerListenAddress));
    }

    public static KafkaProducer<String, String> createProducer(String brokerListenAddress) {
        var props = new Properties();
        props.put("bootstrap.servers", brokerListenAddress);
        props.put("application.id", UUID.randomUUID().toString());
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        var producer = new KafkaProducer<String, String>(props);
        return producer;
    }


    public static EmbeddedKafkaConfig createKafkaConfig(String brokerListenAddress) {
        var brokers = "PLAINTEXT://" + brokerListenAddress;
        HashMap<String, String> config = new HashMap<String, String>();
        config.put("listeners", brokers);
        config.put("advertised.listeners", brokers);
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        EmbeddedKafkaConfig apply = EmbeddedKafkaConfig.apply(6000,
                6001,
                toScalaMap(config),
                toScalaMap(new HashMap<String, String>()),
                toScalaMap(new HashMap<String, String>()));
        return apply;
    }

    public static <A, B> Map<A, B> toScalaMap(java.util.Map<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(
                Predef.<Tuple2<A, B>>conforms()
        );
    }
}
