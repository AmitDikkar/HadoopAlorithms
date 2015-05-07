/**
 * 
 */
package com.hadoop.TfIdf;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

/**
 * @author amitdikkar
 *
 */
public class Stage2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	//test@testFile	4 => <testFile, [test=4]>
	Text documentName = new Text();
	Text wordAndCount = new Text();
	
	@Override
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		
		//Split the line
		String[] lineParts = val.toString().split("\t");
		
		//get "word" from first part
		String word = lineParts[0].split("@")[0];
		//get "document name" from first part
		String docName = lineParts[0].split("@")[1];
		
		//second part is already word count
		String wordCount = lineParts[1];
		
		//lets make output value as word=wordCount
		String outputValue = word + "=" + wordCount;
		
		//set key output key values now.
		documentName.set(docName);
		wordAndCount.set(outputValue);
		
		//send it to reducer
		context.write(documentName, wordAndCount);
	}
}
