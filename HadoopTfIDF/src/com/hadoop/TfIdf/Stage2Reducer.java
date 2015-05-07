/**
 * 
 */
package com.hadoop.TfIdf;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author amitdikkar
 * This is the reducer of second stage.
 * input: <filename, [word1=wordCount1, word2=wordCount2, ....]>
 * output: <word1@filename, word1Count/totalWordCount>
 */
public class Stage2Reducer extends Reducer<Text, Text, Text, Text>{

	Text wordAndDocName = new Text();
	Text wordCountAndTotalCount = new Text();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int totalCount = 0;
		String fileName = key.toString();
		HashMap<String, Integer> tempWordAndWordCounts = new HashMap<String, Integer>();
		
		//iterate through each value which is in the format "word1=wordCount1"
		for(Text val : values){
			
			String[] parts = val.toString().split("=");
			String word = parts[0];
			int wordCount = Integer.parseInt(parts[1]);
			tempWordAndWordCounts.put(word, wordCount);
			totalCount += wordCount;
		}
		
		//iterate through hashmap which has entries in the format <word, wordCount>
		//create output as: <word1@fileName, wordCount1/totalCount>
		for(String word : tempWordAndWordCounts.keySet()){
			wordAndDocName.set(word + "@" + fileName);
			wordCountAndTotalCount.set(tempWordAndWordCounts.get(word) + "/" + totalCount);
			context.write(wordAndDocName, wordCountAndTotalCount);
		}
	}
}