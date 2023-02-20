package wind.extra;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.InvalidRecordException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.utils.Utils;

import java.util.List;
import java.util.Map;

public class HeavyProducerPartitioner implements Partitioner {
    public static final String NUM_HEAVY_PRODUCER_PARTITIONS = "NUM_HEAVY_PRODUCER_PARTITIONS";
    private int numHeavyProducerPartitions = 4;

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey(NUM_HEAVY_PRODUCER_PARTITIONS)) {
            numHeavyProducerPartitions = (int) configs.get(NUM_HEAVY_PRODUCER_PARTITIONS);
        }
    }

    @Override
    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();

        if (numPartitions <= numHeavyProducerPartitions) {
            throw new ConfigException("Topic must have at least " + numHeavyProducerPartitions + " partitions!");
        }
        if (!(key instanceof String)) {
            throw new InvalidRecordException("Keys must be Strings!");
        }
        String strKey = (String) key;
        int partition;
        if (strKey.startsWith("HeavyProducer_")) {
            partition = Math.abs(Utils.murmur2(keyBytes) % numHeavyProducerPartitions);
        } else {
            partition = Math.abs(Utils.murmur2(keyBytes) % (numPartitions - numHeavyProducerPartitions)) + numHeavyProducerPartitions;
        }
        //System.out.println("Partition " + partition + " <- key " + strKey);
        return partition;
    }

    @Override
    public void close() {
    }

}
