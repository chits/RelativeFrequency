import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * User: Chitrali Rai
 */
public class Combiner extends Reducer<WordNeighbor,IntWritable,WordNeighbor,IntWritable> {
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void reduce(WordNeighbor key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
             count += value.get();
        }
        totalCount.set(count);
        context.write(key,totalCount);
    }
}
