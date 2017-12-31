package com.youtube.top5category;

import java.io.IOException;
import java.util.HashMap;

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


public class MaxNumofVideoUpload {
	
	public static class MaxNumofVideoUploadMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
			
	      private Text category = new Text();
	      private final static IntWritable one = new IntWritable(1);
	      
	      public void map(LongWritable key, Text value, Context context) 
	    				throws IOException, InterruptedException
	      {
	    	  
	           String line = value.toString();
	           String str[]=line.split("\t");
	          if(str.length > 5){
	                category.set(str[3]);
	      }
	      context.write(category, one);
	}
  }
	
	public static class MaxNumofVideoUploadReducer extends Reducer<Text, IntWritable,Text,IntWritable>
	{
		
		 public void reduce(Text key, Iterable<IntWritable> values,Context context) 
				 	throws IOException, InterruptedException {
	           int sum = 0;
	           for (IntWritable val : values) {
	               sum += val.get();
	           }	           	          
	           HashMap<String, Integer> redout = new HashMap<String, Integer>();
	           context.write(key, new IntWritable(sum));
	           
	           
		 }
		
	}
	
	public static void main(String args[]) throws Exception 
	{
	
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "MR Join");
		
		job.setJarByClass(MaxNumofVideoUpload.class);
		job.setMapperClass(MaxNumofVideoUploadMapper.class);
		job.setReducerClass(MaxNumofVideoUploadReducer.class);
      		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true) ? 0 :1 );
	
	}
	
	
	
}