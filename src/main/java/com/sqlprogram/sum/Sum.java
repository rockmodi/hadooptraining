package com.sqlprogram.sum;

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

public class Sum {
	
	public static class SumMapper extends 
	   Mapper<LongWritable, Text, Text, IntWritable> 
	{		
		public void map(LongWritable key, Text value, Context context)
		 throws IOException, InterruptedException
		{
			String[] str = value.toString().split(",");
						
			int salary = Integer.parseInt(str[2]);
				
			context.write(new Text("Rock"), new IntWritable(salary));							  							
		}		
	}
	
	public static class SumReducer extends
	  Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException
		{
			int sum = 0;
			for (IntWritable val : values) {
				sum = sum + val.get();
			}			
			context.write(key, new IntWritable(sum));			
		}		
	}	
		
	// Select gender, sum(sal) from employee group by gender -- sql
	public static void main (String args[])
		throws Exception
		{
		  Configuration conf = new Configuration();
		  Job job = Job.getInstance(conf, "sql sum");
		  
		  job.setJarByClass(Sum.class);
		  job.setMapperClass(SumMapper.class);
		  job.setReducerClass(SumReducer.class);
		  		  
		  job.setOutputKeyClass(Text.class);
		  job.setOutputValueClass(IntWritable.class);
			
		  FileInputFormat.addInputPath(job, new Path(args[0]));
		  FileOutputFormat.setOutputPath(job, new Path(args[1]));
		  System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		}
}
