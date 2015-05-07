/**
 * 
 */
package com.hadoop.countpairs;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author amitdikkar
 *
 */
public  class CountPairsReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
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
