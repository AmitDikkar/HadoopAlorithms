/**
 * 
 */
package com.hadoop.wordcount;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

 public class WordCount {

 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        StringTokenizer tokenizer = new StringTokenizer(line);
        InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        String fileName = fileSplit.getPath().getName();
        
        String lineOffset = key.toString();
        while (tokenizer.hasMoreTokens()) {
            word.set(tokenizer.nextToken());
            String outputValue = fileName + "@" +lineOffset;
            Text outputValueText = new Text(outputValue);
            context.write(word, outputValueText);
        }
    }
 } 

 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
        int sum = 0;
        StringBuilder builder = new StringBuilder();
        for (Text val : values) {
        	builder.append(val.toString());
        	builder.append(',');
            //sum += val.get();
        }
        //System.out.println("Value is:" + builder.toString());
        context.write(key, new Text(builder.toString()));
    }
 }

 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    System.out.println("Created configuration");
    Job job = new Job(conf, "wordcount");
    System.out.println("Created Job");
    job.setJarByClass(WordCount.class);
    System.out.println("Set WordCount.jar class");
    
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);

    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);
 }

}