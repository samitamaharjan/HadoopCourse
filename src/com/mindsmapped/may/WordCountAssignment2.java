package com.mindsmapped.may;

import java.io.IOException;

import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordCountAssignment2 {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1); // type of output value
    private Text word = new Text();   // type of output key
      
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString()); // line to string token
      
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());    // set word as each input keyword
        context.write(word, one);     // create a pair <keyword, 1>
      }
    }
  }
  
  public static class Reduce
       extends Reducer<Text,IntWritable,Text,IntWritable> {

    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values, 
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0; // initialize the sum for each keyword
      for (IntWritable val : values) {
        sum += val.get();  
      }
      result.set(sum);

      context.write(key, result); // create a pair <keyword, number of occurences>
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); 
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
  /*if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    } */

    // create a job with name "wordcount"
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(WordCountAssignment2.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // uncomment the following line to add the Combiner
    //job.setCombinerClass(Reduce.class);
     

    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    //set the HDFS path of the input data
   //FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path("/home/cloudera/wordcount/text1.txt"));
    // set the HDFS path for the output
    //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/wordcount/wordcountOutput_text1"));
    
    //Set the number of Reducer
    job.setNumReduceTasks(5);
    
      //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
