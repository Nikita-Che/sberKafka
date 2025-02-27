package org.example;

import org.example.lesson1hw.config.KafkaProducerConfig;
import org.example.lesson1hw.entity.Transaction;
import org.example.lesson1hw.producer.TransactionProducer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TransactionProducerTest {

    private static TransactionProducer transactionProducer;

    @BeforeAll
    public static void setUp() {
        Properties properties = new KafkaProducerConfig().kafkaProperties();
        transactionProducer = new TransactionProducer(properties);
    }

    @AfterAll
    public static void tearDown() {
        transactionProducer.close();
    }

    @Test
    public void testSendRandomTransactions() {
        for (int i = 1; i <= 10; i++) {
            Transaction transaction = new Transaction(
                    getRandomTransactionType(),
                    ThreadLocalRandom.current().nextDouble(100, 10000),
                    "ACC" + ThreadLocalRandom.current().nextInt(1000, 9999),
                    OffsetDateTime.now().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME)
            );

            transactionProducer.sendTransaction(transaction);
            assertNotNull(transaction);
        }
    }

    private static String getRandomTransactionType() {
        String[] types = {"DEPOSIT", "WITHDRAWAL", "TRANSFER", "PAYMENT"};
        return types[ThreadLocalRandom.current().nextInt(types.length)];
    }
}
