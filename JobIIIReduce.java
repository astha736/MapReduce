
package alljobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobIIIReduce extends Reducer<Text,Text,Text,Text>{
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int WMAX = Integer.parseInt(context.getJobName());
    ArrayList c = new ArrayList(); 
    
    //String delim = " : ";
    //String Word = "" ,tfidf = "";
    for (Text val : values) {
        String[] data = val.toString().split(":"); 
        if(Character.isUpperCase(data[0].charAt(0))) {
            context.write(new Text(data[0]+":"+key), new Text(data[1]));
        }
        
        else {
            try{
            c.add(new CustomCompare(data[0],Double.valueOf(data[1])));  
            }
            catch (Exception e)
            {
                //Word = "caught_an_exception";
            }
        }
    }    
    
    Collections.sort(c);
    int size = c.size();
    Double d;
    int i;
    Text keypair;
    for(i=0;i<WMAX &&i< size;i++)
    {   
        CustomCompare cp = (CustomCompare)c.get(i);
        if(cp.getString().equals(""))
        {
        keypair = new Text("ERROR: Empty String".concat(":"+key.toString()));
        }
        else 
        keypair = new Text(cp.getString().concat(":"+key.toString()));
        //Text relevance = new Text("one");
        d = cp.getDouble();
        Text relevance = new Text(Double.toString(d));
        context.write(keypair,relevance);
       
    }
    
 }   
}