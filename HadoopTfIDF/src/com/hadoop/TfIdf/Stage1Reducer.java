package com.hadoop.TfIdf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

/**
 * @author amitdikkar
 *	This is the reducer class of second stage.
 *  input: Word@filename, count
 *  output: Word@filename, sum of counts
 */
public class Stage1Reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException {
        int value = 0;
   
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            value += it.next().get();
        }
        try {
            context.write(key, new IntWritable(value));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
