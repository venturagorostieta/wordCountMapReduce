package com.cloudera.examples;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * First example usging hadoop and cloudera. Program will count all words of
 * input files. executing map reduce functions
 * 
 * @author VENTURA
 *
 */
public class WordCount extends Configured implements Tool {

	/**
	 * args[0] path input text files in HDFS args[1] path for ouput results in HDFS
	 * 
	 * @param paramArrayOfString
	 * @throws Exception
	 */
	public static void main(String[] paramArrayOfString) throws Exception {
		int i = ToolRunner.run(new WordCount(), paramArrayOfString);
		System.exit(i);
	}

	public int run(String[] paramArrayOfString) throws Exception {
		Job job = Job.getInstance(getConf(), "wordcount");
		job.setJarByClass(getClass());
		FileInputFormat.addInputPath(job, new Path(paramArrayOfString[0]));
		FileOutputFormat.setOutputPath(job, new Path(paramArrayOfString[1]));
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
		private static final IntWritable one = new IntWritable(1);

		private Text word = new Text();

		private long numRecords = 0L;

		private static final Pattern WORD_BOUNDARY = Pattern.compile("\\s*\\b\\s*");

		public void map(LongWritable param1LongWritable, Text param1Text,
				Mapper<LongWritable, Text, Text, IntWritable>.Context param1Context)
				throws IOException, InterruptedException {
			String str = param1Text.toString();
			Text text = new Text();
			for (String str1 : WORD_BOUNDARY.split(str)) {
				if (!str1.isEmpty()) {
					text = new Text(str1);
					param1Context.write(text, one);
				}
			}
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text param1Text, Iterable<IntWritable> param1Iterable,
				Reducer<Text, IntWritable, Text, IntWritable>.Context param1Context)
				throws IOException, InterruptedException {
			int i = 0;
			for (IntWritable intWritable : param1Iterable)
				i += intWritable.get();
			param1Context.write(param1Text, new IntWritable(i));
		}
	}

}
