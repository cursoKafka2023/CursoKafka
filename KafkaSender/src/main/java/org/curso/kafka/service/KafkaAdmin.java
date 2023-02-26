package org.curso.kafka.service;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import java.util.Collections;
import java.util.Properties;

public class KafkaAdmin {

    private final Admin admin;

    public KafkaAdmin(String bootstrapServers) {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        this.admin = Admin.create(properties);
    }

    public synchronized void createTopic(String topic, int partitions, short replication) {
        admin.createTopics(Collections.singleton(new NewTopic(topic, partitions, replication)));
    }
}

