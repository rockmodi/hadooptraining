package com;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KeyValue {
	
	public static class KeyValueMapper 
	extends Mapper<Text, Text, Text, Text>{
	
	private Text word = new Text();
	public void map(Text key, Text value, Context context) 
			throws IOException, InterruptedException
	{
		StringTokenizer str = new StringTokenizer(value.toString(), ",");
		while(str.hasMoreTokens())
		{
			word.set(str.nextToken());
			context.write(key, word);
		}			
	}		
}

public static class KeyValueReducer
	extends Reducer<Text, Text, Text, Text>{
	
	private Text result = new Text();
	
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException
	{
		String translations = "";
        for (Text val : values)
        {
            translations += "|"+val.toString();
        }
        result.set(translations);
        context.write(key, result);
	}
	
}


public static void main(String args[]) throws Exception
{
	Configuration conf = new Configuration();
	Job job = Job.getInstance(conf, "Key Vlaue");
	job.setJarByClass(KeyValue.class);
	job.setMapperClass(KeyValueMapper.class);
	job.setReducerClass(KeyValueReducer.class);
	job.setOutputKeyClass(Text.class);
	job.setOutputValueClass(Text.class);
	job.setInputFormatClass(KeyValueTextInputFormat.class);
	FileInputFormat.addInputPath(job, new Path(args[0]));
	FileOutputFormat.setOutputPath(job, new Path(args[1]));
	System.exit(job.waitForCompletion(true) ? 0 : 1);
	
}


}
