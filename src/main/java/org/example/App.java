package org.example;

import org.example.lesson1hw.config.KafkaProducerConfig;
import org.example.lesson1hw.entity.Transaction;
import org.example.lesson1hw.producer.TransactionProducer;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

public class App {
    public static void main(String[] args) {

        Properties properties = new KafkaProducerConfig().kafkaProperties();
        TransactionProducer transactionProducer = new TransactionProducer(properties);

        for (int i = 1; i <= 10; i++) {
            Transaction transaction = new Transaction(
                    getRandomTransactionType(),
                    ThreadLocalRandom.current().nextDouble(100, 10000),
                    "ACC" + ThreadLocalRandom.current().nextInt(1000, 9999),
                    OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            );

            transactionProducer.sendTransaction(transaction);
        }

        transactionProducer.close();
    }

    private static String getRandomTransactionType() {
        String[] types = {"DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"};
        return types[ThreadLocalRandom.current().nextInt(types.length)];
    }
}
