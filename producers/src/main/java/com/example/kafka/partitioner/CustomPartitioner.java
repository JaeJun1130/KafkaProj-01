package com.example.kafka.partitioner;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.StickyPartitionCache;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomPartitioner implements Partitioner {
    public static final Logger logger = LoggerFactory.getLogger(CustomPartitioner.class.getName());

    private final StickyPartitionCache stickyPartitionCache = new StickyPartitionCache();

    private String specialKey;

    /**
     * Properties props 셋팅값을 가져올 수 있음.
     *
     * @param configs
     */
    @Override
    public void configure(Map<String, ?> configs) {
        specialKey = configs.get("custom.specialKey").toString();
    }

    /**
     * DefaultPartitioner 대신 Custom 하게 구현해 Partition 위치를 지정할 수 있음.
     *
     * @param topic      The topic name
     * @param key        The key to partition on (or null if no key)
     * @param keyBytes   The serialized key to partition on( or null if no key)
     * @param value      The value to partition on or null
     * @param valueBytes The serialized value to partition on or null
     * @param cluster    The current cluster metadata
     * @return partition
     */
    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        // Partition 정보를 가진 List 를 반환한다.
        List<PartitionInfo> partitionInfos = cluster.partitionsForTopic(topic);
        int partitionIndex = 0;

        int numPartitions = partitionInfos.size();
        int numSpecialPartitions = (int) (numPartitions * 0.5);

        if (keyBytes == null) {
//            return stickyPartitionCache.partition(topic, cluster);
            throw new InvalidRecordException("key should not be null");
        }

        if (String.valueOf(key).equals(specialKey)) {
            partitionIndex = Utils.toPositive(Utils.murmur2(valueBytes)) % numSpecialPartitions;
        } else {
            partitionIndex = Utils.toPositive(Utils.murmur2(keyBytes)) % (numPartitions - numSpecialPartitions) + numSpecialPartitions;
        }

        logger.info("key:{} is sent to partition:{}", key.toString(), partitionIndex);

        return partitionIndex;
    }

    @Override
    public void close() {

    }
}
