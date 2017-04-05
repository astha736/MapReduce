
package alljobs;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobIIIMap extends Mapper<Object, Text, Text, Text>{
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] data = value.toString().split("\t");   
        String[] wordAndFile = data[0].split(":");
        context.write(new Text(wordAndFile[1]), new Text(wordAndFile[0] + ":" + data[1]));
    }
}
