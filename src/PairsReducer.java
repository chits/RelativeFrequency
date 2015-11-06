import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * User: Chitrali Rai
 */
public class PairsReducer extends Reducer<WordNeighbor, IntWritable, Text, Text> {
    private double totalCount;
    private double relativeCount;
    private Text currentWord = new Text("NOT_SET");
    private Text flag = new Text("*");
    List<Pair> pairList = new ArrayList<>();

    @Override
    protected void reduce(WordNeighbor key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        if (key.getNeighbor().equals(flag)) {
            if (key.getWord().equals(currentWord)) {
                totalCount = totalCount + getTotalCount(values);
            } else {
                currentWord.set(key.getWord());
                totalCount = 0;
                totalCount = (getTotalCount(values));
            }
        } else {
            int count = getTotalCount(values);
            relativeCount = (double) count / totalCount;
            context.write(new Text(key.toString()), new Text(relativeCount + ""));
        }

    }
//    
//    protected void cleanup(Context context)
//    throws IOException,
//    InterruptedException 
//    {	
//    	Collections.sort(pairList, new CustomComparator());
//    	Collections.reverse(pairList);
//    	for(int i = 0; i < pairList.size() ; i++)
//    	{
//    		Pair pair = pairList.get(i);
//    		context.write(new Text(pair.key), new Text(pair.relativeFrequency + ""));
//    	}
//    }
    
    public class CustomComparator implements Comparator<Pair> {
        @Override
        public int compare(Pair o1, Pair o2) {
        	if (o1.relativeFrequency < o2.relativeFrequency) return -1;
            if (o1.relativeFrequency > o2.relativeFrequency) return 1;
            return 0;
        }
    }
    
    class Pair{
    	double relativeFrequency;
    	String key;

    	Pair(double relativeFrequency, String key) {
    	this.relativeFrequency = relativeFrequency;
    	this.key = key;
    	}
    }

    private int getTotalCount(Iterable<IntWritable> values) {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        return count;
    }
}
