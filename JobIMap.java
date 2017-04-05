package alljobs;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.StringUtils;

public class JobIMap extends Mapper<Object, Text, Text, IntWritable> {
    
        static enum CountersEnum { INPUT_WORDS,TOTAL_WORDS}
        
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
        
        private HashSet patternsToSkip = new HashSet(); //not sure if hash of hadoop or java 
        
        private BufferedReader fis;
        private Configuration conf;
        
@Override
public void setup(Context context) throws IOException,
 InterruptedException {
    conf = context.getConfiguration();
    
if (conf.getBoolean("wordcount.skip.patterns", true)) {
    URI[] patternsURIs = Job.getInstance(conf).getCacheFiles();
    for (URI patternsURI : patternsURIs) {
    Path patternsPath = new Path(patternsURI.getPath());
   String patternsFileName = patternsPath.getName().toString();
   
   parseSkipFile(patternsFileName);
 }
}
}

private void parseSkipFile(String fileName) {
try {
fis = new BufferedReader(new FileReader(fileName));
String pattern = null;
 while ((pattern = fis.readLine()) != null) {
   StringTokenizer itr = new StringTokenizer(pattern);
       while(itr.hasMoreTokens()){
       patternsToSkip.add((itr.nextToken()));
        } 
 }
} catch (IOException ioe) {
 System.err.println("Caught exception while parsing the cached file '" + StringUtils.stringifyException(ioe));
}

}

@Override
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
    
    
    String line = value.toString();
    
    int phraseLength = 0;
    StringBuilder phraseBuilder = new StringBuilder();

    String pun = ", . ,  / : ; \" - ' \\ ] [ } { ) ( ? 1 2 3 4 5 6 7 8 9 0 & ! @ # $ % ^ * _ + = > < / ";
    String pattern;
    StringTokenizer token = new StringTokenizer(pun);
    while(token.hasMoreTokens()){
       pattern = token.nextToken().toString();
       line = line.replace(pattern, " ");
       }
    
    StringTokenizer itr = new StringTokenizer(line);
        while (itr.hasMoreTokens()) {
        	String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); 
        	StringBuilder valueBuilder = new StringBuilder();
                String str = itr.nextToken().toString();
                if(patternsToSkip.contains(str.toLowerCase())){
                context.getCounter(CountersEnum.TOTAL_WORDS).increment(1);
                if(Character.isLowerCase(str.charAt(0))) {
                    if(phraseLength>1) {
                    phraseBuilder.setCharAt((phraseBuilder.length() - 1), ':');
                    phraseBuilder.append(fileName);
                    context.write(new Text(phraseBuilder.toString()), new IntWritable(1000)); }
                    phraseBuilder.setLength(0);
                    phraseLength = 0;
                  }
                }
                else{
        	word.set(str);
                valueBuilder.append(word);
                valueBuilder.append(":");
                valueBuilder.append(fileName);
                // emit the partial <k,v>context
                context.write(new Text(valueBuilder.toString()), one);
                //context.write(word, one);
                if(Character.isUpperCase(word.charAt(0))) {
            	phraseBuilder.append(word);
            	phraseBuilder.append(" ");
            	phraseLength++;
                }
                else {
            	if(phraseLength>1) {
                phraseBuilder.setCharAt((phraseBuilder.length() - 1), ':');
                phraseBuilder.append(fileName);
                context.write(new Text(phraseBuilder.toString()), new IntWritable(1000)); }
            	phraseBuilder.setLength(0);
            	phraseLength = 0;
                }
                //Check if entire file is capitalized.

                Counter counter = context.getCounter(CountersEnum.class.getName(),
                CountersEnum.INPUT_WORDS.toString());
                counter.increment(1);
                context.getCounter(CountersEnum.TOTAL_WORDS).increment(1);
                }
        }
	}
	
}
