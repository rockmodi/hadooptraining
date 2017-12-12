package com;

import java.io.IOException;
//import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SequenceInput {
	
	public static class SequenceMapper 
				extends Mapper<LongWritable, Text, LongWritable, Text> {
		
		public void map(LongWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			
			context.write(key, value);
			
		}
	}
	
		
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = new Job(conf, "SequenceInput");
		job.setJarByClass(SequenceInput.class);
		job.setMapperClass(SequenceMapper.class);
		//job.setReducerClass(SequenceReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(0);
		System.exit(job.waitForCompletion(true) ? 0: 1);
		
	}
	

}
