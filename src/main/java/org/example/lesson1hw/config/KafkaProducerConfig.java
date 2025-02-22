package org.example.lesson1hw.config;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class KafkaProducerConfig {

    public Properties kafkaProperties() {
        Properties props = new Properties();

        try (InputStream input = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (input == null) {
                throw new RuntimeException("Не удалось найти application.properties");
            }
            props.load(input);

            props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty("kafka.bootstrap.servers"));
            props.put(ProducerConfig.ACKS_CONFIG, props.getProperty("kafka.acks"));
            props.put(ProducerConfig.RETRIES_CONFIG, Integer.parseInt(props.getProperty("kafka.retries")));
            props.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(props.getProperty("kafka.linger.ms")));
            props.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(props.getProperty("kafka.batch.size")));

            props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.getProperty("kafka.key.serializer"));
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.getProperty("kafka.value.serializer"));
            props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, props.getProperty("kafka.partitioner.class"));

        } catch (IOException e) {
            throw new RuntimeException("Ошибка загрузки application.properties", e);
        }

        return props;
    }
}
