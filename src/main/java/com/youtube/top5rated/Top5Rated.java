package com.youtube.top5rated;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top5Rated {

	 public static class Top5RatedMapper extends Mapper<LongWritable, Text, Text, FloatWritable> 
	 {
	     private Text video_name = new Text();
		 private  FloatWritable rating = new FloatWritable();
		 
		 public void map(LongWritable key, Text value, Context context )
		      throws IOException, InterruptedException 
		 {
			 String line = value.toString();
			 if(line.length()>0) {
				 String str[]=line.split("\t");
				 video_name.set(str[0]);
				 if(str.length > 6)
				 {
					 if(str[6].matches("\\d+.+")){ 
						 float f=Float.parseFloat(str[6]); 
						 rating.set(f);
					 }
				 }
			 }
			 context.write(video_name, rating);
		 }
	 }
	 
	 public static class Top5RatedReducer extends Reducer<Text, FloatWritable,Text, FloatWritable>
		{
			
			 public void reduce(Text key, Iterable<FloatWritable> values,Context context) 
					 	throws IOException, InterruptedException {
		           float sum = 0;
		           int l = 0;
		           for (FloatWritable val : values) {
		                l+=1; 
		               sum += val.get();
		           	}
		           	sum=sum/l; 
		           	context.write(key, new FloatWritable(sum));
		           
			 }
			
		}
		
		public static void main(String args[]) throws Exception 
		{
		
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Top5Rated");
			
			job.setJarByClass(Top5Rated.class);
			job.setMapperClass(Top5RatedMapper.class);
			job.setReducerClass(Top5RatedReducer.class);
	      		
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(FloatWritable.class);
			
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			
			System.exit(job.waitForCompletion(true) ? 0 :1 );
		}
		
		  
}
