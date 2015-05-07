/**
 * 
 */
package com.hadoop.countpairs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.lucene.analysis.en.EnglishAnalyzer;
import org.apache.lucene.analysis.util.CharArraySet;

/**
 * @author amitdikkar
 *
 */

public class CountPairsMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable write= new IntWritable(1);
	private Text words = new Text();

	@Override
	public void map(LongWritable key, Text val, Context context) throws IOException {
		String line = val.toString();
		ArrayList<String> wordsArray = new ArrayList<String>();
		ArrayList<String> newArray = new ArrayList<String>();
		String conString ="";
		line = getCorrectLine(line);
/*		line = line.toLowerCase();

		line = line.replaceAll("[^a-zA-Z0-9 ]", " ").trim();*/

		StringTokenizer token = new StringTokenizer(line);
		while (token.hasMoreTokens()) {
			words.set(token.nextToken());
			wordsArray.add(words.toString());
		}
		
		HashSet<String> keysOfReducer = new HashSet<String>();
		
		for(int var=0; var< wordsArray.size(); var++){
			for(int var1=0; var1< wordsArray.size(); var1++){
				if(var!=var1){
					String firstWord = wordsArray.get(var);
					String secondWord = wordsArray.get(var1);
					//if(firstWord.equals("sherlock") || secondWord.equals("sherlock")){
						if(firstWord.compareTo(secondWord) < 0){
							conString = firstWord +","+ secondWord;
						}
						else {
							conString = secondWord +","+ firstWord;
						}
						//if
						if(keysOfReducer.contains(conString)){
							;
						}
						else{
							keysOfReducer.add(conString);
							newArray.add(conString);
						}
						//System.out.println(conString);
					//}
				}
			}
		}

		for(int j=0; j< newArray.size(); j++){
			words.set(newArray.get(j));
			try {
				context.write(words,write);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	private String getCorrectLine(String line) throws IOException {
		line = line.toLowerCase();
		line = removeStopWords(line);
		line = line.replaceAll("[^a-zA-Z0-9 ]", " ").trim();
		return line;
	}

	private static String removeStopWords(String line) throws IOException {
		HashSet<String> stopWords = new HashSet<String>();
		stopWords.add("I"); stopWords.add("a"); stopWords.add("about");
		stopWords.add("an"); stopWords.add("are"); stopWords.add("as");
        stopWords.add("at"); stopWords.add("be"); stopWords.add("by");
        stopWords.add("com"); stopWords.add("de"); stopWords.add("en");
        stopWords.add("for"); stopWords.add("from"); stopWords.add("how");
        stopWords.add("in"); stopWords.add("is"); stopWords.add("it");
        stopWords.add("la"); stopWords.add("of"); stopWords.add("on");
        stopWords.add("or"); stopWords.add("that"); stopWords.add("the");
        stopWords.add("this"); stopWords.add("to"); stopWords.add("was");
        stopWords.add("what"); stopWords.add("when"); stopWords.add("where"); 
        stopWords.add("who"); stopWords.add("will"); stopWords.add("with");
        stopWords.add("and"); stopWords.add("the"); stopWords.add("www");
        String[] array = line.split(" ");
        StringBuilder builder = new StringBuilder();
        for (String str : array){
        	if(!stopWords.contains(str)){
        		builder.append(str + " ");
        	}
        }
        return builder.toString();
	}
}