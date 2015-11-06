import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * User: Chitrali Rai
 */
public class WordNeighborPartitioner extends Partitioner<WordNeighbor,IntWritable> {

    @Override
    public int getPartition(WordNeighbor wordPair, IntWritable intWritable, int numPartitions) {
        return Math.abs(wordPair.getWord().hashCode()) % numPartitions;
    }
}
