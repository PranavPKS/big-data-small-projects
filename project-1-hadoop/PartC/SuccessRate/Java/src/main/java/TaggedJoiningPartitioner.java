import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.io.*;

public class TaggedJoiningPartitioner extends Partitioner<TaggedKey, Text> {

    public int getPartition(TaggedKey taggedKey, Text text, int numPartitions) {
        return (taggedKey.getJoinKey().hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}