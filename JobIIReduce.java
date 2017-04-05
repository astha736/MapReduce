package alljobs;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobIIReduce extends Reducer<Text, Text, Text, DoubleWritable> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        int corpusSize = Integer.parseInt(context.getJobName());

	int noOfDocsWithKey = 0;
		
        Map<String, String> tempFrequencies = new HashMap<String, String>();
        
        for (Text val : values) {
            String[] documentAndFrequencies = val.toString().split(":");
            noOfDocsWithKey++;
            tempFrequencies.put(documentAndFrequencies[0], documentAndFrequencies[1]);
        }
        
        for (String document : tempFrequencies.keySet()) {
            String wordCount = tempFrequencies.get(document);
 
            //tf(W,F) is the number of occurrences of W in F
            double tf = Double.valueOf(wordCount);
 
            //Idf(W,C) is the inverse of the proportion of files in C in which W occurs (at least once).
            double idf = (double) corpusSize / (double) noOfDocsWithKey;
 
            //tf_idf(W,F) = tf(W,F) * log(idf(W,C)) for F in C
            double tf_idf = tf * Math.log10(idf);
 
            context.write(new Text(key + ":" + document), new DoubleWritable(tf_idf));
        }
    }	
}
