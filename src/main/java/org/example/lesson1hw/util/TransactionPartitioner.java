package org.example.lesson1hw.util;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class TransactionPartitioner implements Partitioner {

    private static final Logger log = LoggerFactory.getLogger(TransactionPartitioner.class);

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        if (!(key instanceof String)) {
            log.warn("Ключ должен быть строкой. Используется дефолтная партиция.");
            return 0;
        }

        String transactionType = (String) key;
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        int partition;

        switch (transactionType.toUpperCase()) {
            case "DEPOSIT":
                partition = 0;
                break;
            case "WITHDRAWAL":
                partition = 1;
                break;
            case "TRANSFER":
                partition = 2;
                break;
            case "PAYMENT":
                partition = 3;
                break;
            default:
                partition = Math.abs(transactionType.hashCode()) % numPartitions;
                break;
        }

        log.info("Определена партиция: {} для операции: {}", partition, transactionType);
        return partition;
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
