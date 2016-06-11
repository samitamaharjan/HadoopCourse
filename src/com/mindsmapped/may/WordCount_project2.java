package com.mindsmapped.may;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount_project2 {

  public static class Map 
            extends Mapper<LongWritable, Text, Text, IntWritable>{
	
    public void map(LongWritable key, Text value, Context context
                    ) throws IOException, InterruptedException {
      String text = value.toString().toLowerCase();
      String[] arr = text.split("([^A-Za-z0-9\\-]+)|([A-Za-z0-9.\\-_]+@[A-Za-z0-9.\\-]+)|(http[s]?:\\/\\/[A-Za-z0-9.\\-_/:]+)|(([+]?[0-9]{1,3}){0,1}[ ]?[\\(]?[0-9]{1,3}[\\)]?[ \\-]?[0-9]{3}[ \\-]?[0-9]{4})");

      for (int i = 0; i <=  arr.length - 3; i++) {
    	  String str1 = arr[i];
    	  String str2 = arr[i+1];
    	  String str3 = arr[i+2];
    	  
    	  if (str1 != null && str1.length() > 0 && str2 != null && str2.length() > 0 ){
    		  context.write(new Text(str1 + " " + str2 + " " + str3), new IntWritable(1)) ;
    	  }
    	
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
      if (sum > 1){
    	  context.write(key, result); // create a pair <keyword, number of occurences>
      }
    }
  }

  // Driver program
  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration(); 
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs(); // get all args
    /*if (otherArgs.length != 2) {
      System.err.println("Usage: WordCount <in> <out>");
      System.exit(2);
    }*/

    // create a job with name "wordcount"
    Job job = new Job(conf, "wordcount");
    job.setJarByClass(WordCount_project2.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
   
    // uncomment the following line to add the Combiner
    job.setCombinerClass(Reduce.class);
     

    // set output key type   
    job.setOutputKeyClass(Text.class);
    // set output value type
    job.setOutputValueClass(IntWritable.class);
    //set the HDFS path of the input data
    // FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    // set the HDFS path for the output
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
      //Wait till job completion
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

