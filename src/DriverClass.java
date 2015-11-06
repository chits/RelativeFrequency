import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * User: Chitrali Rai
 */
public class DriverClass {

    public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
        Job job = Job.getInstance(new Configuration());
        job.setJarByClass(DriverClass.class);
        job.setJobName("Relative_Frequency");

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]+ "/rfpairs"));

        job.setMapperClass(PairsMapper.class);
        job.setReducerClass(PairsReducer.class);
        job.setCombinerClass(Combiner.class);
        job.setPartitionerClass(WordNeighborPartitioner.class);
        job.setNumReduceTasks(4);

        job.setOutputKeyClass(WordNeighbor.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
