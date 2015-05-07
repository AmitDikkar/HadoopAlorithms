/**
 * 
 */
package com.hadoop.TfIdf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author amitdikkar
 *
 */
public class Stage3Mapper extends Mapper<LongWritable, Text, Text, Text>{
	 private Text wordAndDoc = new Text();
     private Text wordAndCounters = new Text();

     /**
      *     Input: word@fileName    wordCount/totalWordCount
      *     Output: Word, fileName=wordCount/totalWordCount,1
      */
     public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
         String[] wordAndCounters = value.toString().split("\t");
         String[] wordAndDoc = wordAndCounters[0].split("@");
         this.wordAndDoc.set(new Text(wordAndDoc[0]));
         this.wordAndCounters.set(wordAndDoc[1] + "=" + wordAndCounters[1]);
         context.write(this.wordAndDoc, this.wordAndCounters);
     }
}
