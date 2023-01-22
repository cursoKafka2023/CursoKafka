package org.curso.kafka.service;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaService {

    private final KafkaProducer producer;

    private final String keySerializer;

    private final String valueSerializer;


    public KafkaService(String bootstrapServers, String keySerializer, String valueSerializer) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;

        switch (keySerializer){
            case "Long":
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            case "GenericRecord":
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            default:
                properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        switch (valueSerializer){
            case "Long":
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
            case "GenericRecord":
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
            default:
                properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        producer = new KafkaProducer<>(properties);
    }

    public synchronized void sendMessage(String topic, Integer partition, String key, String value, Iterable<Header> headers, Long timestamp) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, partition, timestamp, key, value, headers);
        producer.flush();
        producer.close();
    }

}
