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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

enum NewCounter1 {
	REJECTED_NUM, ACCEPT_WORD;
}

public class WordCountRejectNum_james {

	public static class Map extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		public static boolean containsDigit(String str) {
			return str.matches("[0-9]+");
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString()); 

			while (itr.hasMoreTokens()) {
				String token = itr.nextToken();
				if (containsDigit(token) == true) {
					context.getCounter(NewCounter1.REJECTED_NUM).increment(1);
				} else {
					context.write(new Text(token), new IntWritable(1));
					context.getCounter(NewCounter1.ACCEPT_WORD).increment(1);
				}
			}
		}

		public static class Reduce extends
				Reducer<Text, IntWritable, Text, IntWritable> {

			public void reduce(Text key, Iterable<IntWritable> values,
					Context context) throws IOException, InterruptedException {
				int sum = 0; // initialize the sum for each keyword
				for (IntWritable val : values) {
					sum += val.get();
				}
				context.write(key, new IntWritable(sum));
			}
		}

		// Driver program
		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String[] otherArgs = new GenericOptionsParser(conf, args)
					.getRemainingArgs(); // get all args
			/*
			 * if (otherArgs.length != 2) {
			 * System.err.println("Usage: WordCount <in> <out>");
			 * System.exit(2); }
			 */

			// create a job with name "wordcount"
			Job job = new Job(conf, "wordcount");
			job.setJarByClass(WordCountRejectNum_james.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);

			// uncomment the following line to add the Combiner
			job.setCombinerClass(Reduce.class);

			// set output key type
			job.setOutputKeyClass(Text.class);
			// set output value type
			job.setOutputValueClass(IntWritable.class);
			// set the HDFS path of the input data
			// FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
			FileInputFormat.addInputPath(job, new Path(
					"/home/cloudera/wordcount/text_withNum.txt"));
			// set the HDFS path for the output
			// FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
			FileOutputFormat.setOutputPath(job, new Path(
					"/home/cloudera/wordcount/output_withNum5"));

			// Wait till job completion
			System.exit(job.waitForCompletion(true) ? 0 : 1);
		}
	}
}
