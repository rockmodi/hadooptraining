package com.sqlprogram.count;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class Count {
	
	public static class CountMapper extends 
		Mapper <LongWritable, Text, Text, Text> {
	 
		 private final static Text i = new Text("one");
		
		public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException 
		{
			context.write(i ,i);
			
		}		
	}
	
	public static class CountReducer 
	 extends Reducer <Text, Text, Text, Text>{
		
		Text result = new Text();
		Text itr = new Text();
		
		   public void reduce(Text key, Iterable<Text> values,
                   Context context
                   ) throws IOException, InterruptedException {
			   
			int total = 0;
			for (Text val : values) {
				System.out.println(val);
			  	total = total + 1;
			}						
			
			itr.set(Integer.toString(total));
			result.set("");
			context.write(itr, result);
			
		}
		
	}
	
	public static void main(String args[]) throws Exception{
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "countjob");
		
		job.setJarByClass(Count.class);
		job.setMapperClass(CountMapper.class);
		job.setReducerClass(CountReducer.class);
		//job.setNumReduceTasks(0);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
	
	

}
