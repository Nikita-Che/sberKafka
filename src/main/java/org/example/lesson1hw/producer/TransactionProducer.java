package org.example.lesson1hw.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.example.lesson1hw.entity.Transaction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class TransactionProducer {

    private static final Logger log = LoggerFactory.getLogger(TransactionProducer.class);
    private static final String TOPIC = "lesson1hw";

    private final KafkaProducer<String, Transaction> producer;

    public TransactionProducer(Properties properties) {
        this.producer = new KafkaProducer<>(properties);
    }

    public void sendTransaction(Transaction transaction) {
        ProducerRecord<String, Transaction> record = new ProducerRecord<>(TOPIC, transaction.getTransactionType(), transaction);

        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    log.info("Успешная отправка: Топик: {}, Партиция: {}, Смещение: {}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    log.error("Ошибка отправки: Партиция: {}, Смещение: {} - {}",
                            metadata.partition(), metadata.offset(), exception.getMessage());
                }
            }
        });
        producer.flush();
    }

    public void close() {
        log.info("TransactionProducer закрыт");
        producer.close();
    }
}