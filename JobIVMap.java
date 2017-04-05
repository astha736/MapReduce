/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package alljobs;
import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class JobIVMap extends Mapper<Object, Text, Text, Text>{
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        String[] data = value.toString().split("\t");   
        String[] wordAndFile = data[0].split(":");
        context.write(new Text(wordAndFile[0]), new Text(wordAndFile[1] + ":" + data[1]));
        
        /*Text first ,second;
        String Word,File;
        Word = "";
        File = "";
        String line = value.toString();
        String delim = " : ";
        StringTokenizer itr = new StringTokenizer(line,delim,false);
        if(itr.hasMoreTokens())
        {
            Word = itr.nextToken().toString(); // word
        }
        if(itr.hasMoreTokens())
        {
            File = itr.nextToken().toString(); // file
        }
        
        while(itr.hasMoreTokens())
        {
         File.concat(":"+itr.nextToken().toString());  // word tfidf 
        }
        first = new Text(Word);
        second = new Text(File);
        context.write(first,second);
        * 
        */
    }
}
