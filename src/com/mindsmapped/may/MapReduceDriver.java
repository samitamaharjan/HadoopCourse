package com.mindsmapped.may;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.mindsmapped.may.WordCountAssignment3.Reduce;

public class MapReduceDriver {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs(); // get all args
		/*if (otherArgs.length != 2) {
			System.err.println("Usage: WordCount <in> <out>");
			System.exit(2);
		}*/

		// create a job with name "wordcount"
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(MapReduceDriver.class);
		job.setMapperClass(Test_hive.class);
		job.setReducerClass(MyReducer.class);

		// uncomment the following line to add the Combiner
		job.setCombinerClass(MyReducer.class);

		// set output key type
		job.setOutputKeyClass(Text.class);
		// set output value type
		job.setOutputValueClass(IntWritable.class);
		// set the HDFS path of the input data
		//FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileInputFormat.addInputPath(job, new Path("/home/cloudera/wordcount/text2.txt"));
		// set the HDFS path for the output
		//FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path("/home/cloudera/wordcount/output_practice4"));

		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
