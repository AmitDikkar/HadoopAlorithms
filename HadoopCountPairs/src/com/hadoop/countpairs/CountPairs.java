package com.hadoop.countpairs;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

/*
 * Start hadoop = /usr/local/hadoop/sbin/start-all.sh
 * Run Jar: 
 * hadoop jar /home/amitdikkar/Desktop/HadoopPrograms/CountPairs.jar /usr/local/hadoop/newinputfiles/ /usr/local/hadoop/countpairoutput
 * */
public class CountPairs {

	/**
	 * @param args
	 * @throws IOException 
	 * @throws InterruptedException 
	 * @throws ClassNotFoundException 
	 */
	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    System.out.println("Created configuration");
	    Job job = new Job(conf, "wordcount");
	    System.out.println("Created Job");
	    job.setJarByClass(CountPairs.class);
	    System.out.println("Set WordCount.jar class");
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);

	    job.setMapperClass(CountPairsMapper.class);
	    job.setReducerClass(CountPairsReducer.class);

	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));

	    job.waitForCompletion(true);
	}
}
