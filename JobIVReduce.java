/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package alljobs;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class JobIVReduce extends Reducer<Text,Text,Text,Text>{

    
    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
    int FMAX = Integer.parseInt(context.getJobName());
    ArrayList c = new ArrayList(); 
    
    for (Text val : values) {
        
        String[] data = val.toString().split(":"); 
            try{
            c.add(new CustomCompare(data[0],Double.valueOf(data[1])));  
            }
            catch (Exception e)
            {
                //Word = "caught_an_exception";
            }
    }    
    
    Collections.sort(c);
    int size = c.size();
    Double d;
    int i;
    String keypair = key.toString()+":";
    String FILES = "";
    
    for(i=0;i< FMAX &&i< size;i++)
    {   
        CustomCompare cp = (CustomCompare)c.get(i);
        if(cp.getString().equals(""))
        {
        //FILES = FILES.concat("ERROR: Empty String");
        }
        else 
        FILES = FILES.concat(cp.getString()+" ");
        //Text relevance = new Text("one");
        
       
    }
        
        context.write(new Text(keypair),new Text(FILES));
      
    }
    
 }   
