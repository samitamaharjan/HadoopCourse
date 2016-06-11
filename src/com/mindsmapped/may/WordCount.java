package com.mindsmapped.may;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

enum User_Defined_Counter_New{
 REJECTED,
 MALFORMED,
 NUB_RECORDS_INMAP,
 TEXT_DATA,
 VOICE_DATA
 }


public class WordCount {

	public static class MyPartition extends Partitioner<Text, IntWritable> {

		@Override
		public int getPartition(Text text, IntWritable value, int arg2) {
			// TODO Auto-generated method stub

			if (text.toString().equals("to")) {
				return 1;
			} else if (text.toString().equals("be")) {
				return 2;
			}else if (text.toString().equalsIgnoreCase("THIS")){
				return 3;
			}else if (text.toString().equalsIgnoreCase("line")){
				return 4;
			}
			return 0;
		}

	}

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1); // type of
																	// output
																	// value
		private Text word = new Text(); // type of output key

		public void map(LongWritable key, Text value, Context context/*
																	 * ,Reporter
																	 * report
																	 */
		) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); 
																	

			while (itr.hasMoreTokens()) {
				/*String val = itr.nextToken().toLowerCase();*/
				String val = itr.nextToken();
				if (val.equals("is")) {
					
					  context.getCounter(User_Defined_Counter_New.REJECTED).increment
					  (1);
					 
				} else {
					word.set(val); // set word as each input keyword
					context.write(word, one); // create a pair <keyword, 1>
					
					 context.getCounter(User_Defined_Counter_New.NUB_RECORDS_INMAP)
					  .increment(1);
					 
				}
			}
		}
	}

	public static class Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {
			int sum = 0; // initialize the sum for each keyword
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result); 
		}
	}

	// Driver program
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		/*
		 * if (otherArgs.length != 2) {
		 * System.err.println("Usage: WordCount <in> <out>"); System.exit(2); }
		 */

		// create a job with name "wordcount"
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCount.Map.class);
		job.setPartitionerClass(WordCount.MyPartition.class);
		job.setReducerClass(WordCount.Reduce.class);
		job.setNumReduceTasks(5);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		/*FileInputFormat.addInputPath(job, new Path(
				"/home/cloudera/Desktop/Input/demo.txt"));*/
		
		
		// set the HDFS path for the output
		 FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		 FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

	/*	FileOutputFormat.setOutputPath(job, new Path(
				"/home/cloudera/Desktop/MapReduce/Output_Counter_New"));*/

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
