/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package alljobs;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * @author astha
 */
public class AllJobs {

    /**
     * @param args the command line arguments
     */
    public static int main(String[] args) throws Exception {
    
    final String INT_PATH1 = args[1].substring(0, args[1].lastIndexOf('/')+1)+"Intermediate_Output_1";
    final String INT_PATH2 = args[1].substring(0, args[1].lastIndexOf('/')+1)+"Intermediate_Output_2";
    final String INT_PATH3 = args[1].substring(0, args[1].lastIndexOf('/')+1)+"Intermediate_Output_3";
        
    Configuration conf = new Configuration();
    Job job1 = Job.getInstance(conf, "Get Word Count");
    FileSystem fs = FileSystem.get(conf);
    GenericOptionsParser optionParser = new GenericOptionsParser(conf, args);
    String[] remainingArgs = optionParser.getRemainingArgs();
    /*if (!(remainingArgs.length != 2 || remainingArgs.length != 4)) {
      System.err.println("Usage: wordcount <in> <out> [-skip skipPatternFile]");
      System.exit(2);
    }*/
    job1.setJarByClass(AllJobs.class);
    job1.setMapperClass(JobIMap.class);
    job1.setCombinerClass(JobIReduce.class);
    job1.setReducerClass(JobIReduce.class);
    job1.setOutputKeyClass(Text.class);
    job1.setOutputValueClass(IntWritable.class);

    //System.out.println("trying to figure out skip");
    List<String> otherArgs = new ArrayList<String>();
    for (int i=0; i < remainingArgs.length; ++i) {
      if ("-skip".equals(remainingArgs[i])) {
        job1.addCacheFile(new Path(remainingArgs[++i]).toUri());
        job1.getConfiguration().setBoolean("wordcount.skip.patterns", true);
      } else {
        otherArgs.add(remainingArgs[i]);
      }
    }
    FileInputFormat.addInputPath(job1, new Path(otherArgs.get(0)));
    FileOutputFormat.setOutputPath(job1, new Path(INT_PATH1));
    job1.waitForCompletion(true);
    
    Configuration conf2 = new Configuration();
    Job job2 = Job.getInstance(conf2, "Get TF_IDF"); 
    job2.setJarByClass(AllJobs.class);
    job2.setMapperClass(JobIIMap.class);
    job2.setReducerClass(JobIIReduce.class);
    job2.setMapOutputKeyClass(Text.class);
    job2.setMapOutputValueClass(Text.class);
    job2.setOutputKeyClass(Text.class);
    job2.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.addInputPath(job2, new Path(INT_PATH1));
    FileOutputFormat.setOutputPath(job2, new Path(INT_PATH2));
    Path inputPath = new Path(args[0]);
    FileSystem fs2 = inputPath.getFileSystem(conf2);
    FileStatus[] stat = fs2.listStatus(inputPath);
    job2.setJobName(String.valueOf(stat.length));
    job2.waitForCompletion(true);
    
  
    Configuration conf3 = new Configuration();
    Job job3 = Job.getInstance(conf3, "Get WMAX words"); 
    job3.setJarByClass(AllJobs.class);
    job3.setMapperClass(JobIIIMap.class);
    job3.setReducerClass(JobIIIReduce.class);
    job3.setOutputKeyClass(Text.class);
    job3.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job3, new Path(INT_PATH2));
    FileOutputFormat.setOutputPath(job3, new Path(INT_PATH3));
    job3.setJobName(args[4]);
    job3.waitForCompletion(true);  
    
    Configuration conf4 = new Configuration();
    Job job4 = Job.getInstance(conf4, "Get FMAX files"); 
    job4.setJarByClass(AllJobs.class);
    job4.setMapperClass(JobIVMap.class);
    job4.setReducerClass(JobIVReduce.class);
    job4.setMapOutputKeyClass(Text.class);
    job4.setMapOutputValueClass(Text.class);
    job4.setOutputKeyClass(Text.class);
    job4.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job4, new Path(INT_PATH3));
    FileOutputFormat.setOutputPath(job4, new Path(args[1]));
    job4.setJobName(String.valueOf(args[5]));
    return (job4.waitForCompletion(true) ? 0 : 1); 
    }
}
