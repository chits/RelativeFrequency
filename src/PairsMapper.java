import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * User: Chitrali Rai
 */
public class PairsMapper extends Mapper<LongWritable, Text, WordNeighbor, IntWritable> {
    private WordNeighbor wordNeighbor = new WordNeighbor();
    private IntWritable ONE = new IntWritable(1);
    private IntWritable totalCount = new IntWritable();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        int neighbors = context.getConfiguration().getInt("neighbors", 1);
        String[] tokens = value.toString().split("\\s+");
        if (tokens.length > 1) {
            for (int i = 0; i < tokens.length; i++) {
            	if(tokens[i].matches("^[A-Za-z]+$"))
            	{
                    tokens[i] = tokens[i].replaceAll("\\W+","");

                    if(tokens[i].equals("")){
                        continue;
                    }

                    wordNeighbor.setWord(tokens[i]);

                    int start = (i - neighbors < 0) ? 0 : i - neighbors;
                    int end = (i + neighbors >= tokens.length) ? tokens.length - 1 : i + neighbors;
                    for (int j = start; j <= end; j++) {
                        if (j == i) continue;
                        if(tokens[j].matches("^[A-Za-z]+$"))
                        {
                            wordNeighbor.setNeighbor(tokens[j].replaceAll("\\W",""));
                            context.write(wordNeighbor, ONE);
                        }
                    }
                    wordNeighbor.setNeighbor("*");
                    totalCount.set(end - start);
                    context.write(wordNeighbor, totalCount);
            	}
            }
        }
    }
}
