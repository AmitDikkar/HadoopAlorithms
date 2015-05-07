package com.hadoop.TfIdf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.StringTokenizer;

import javax.swing.text.html.HTMLDocument.HTMLReader.FormAction;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


/**
 * @author amitdikkar
 *	This is the mapper class of first stage of TF-IDF algo.
 *	
 *  Input of mapper: single line in Text format.
 *  Output of mapper:  Word@filename, count of the word.
 */
public class Stage1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private final static IntWritable one = new IntWritable(1);
	private Text words = new Text();

	@Override
	public void map(LongWritable key, Text val, Context context) throws IOException, InterruptedException {
		
		String line = val.toString();

		line = getCorrectLine(line);

		StringTokenizer token = new StringTokenizer(line);
		
		InputSplit inputSplit = context.getInputSplit();
        FileSplit fileSplit = (FileSplit) inputSplit;
        String fileName = fileSplit.getPath().getName();
        StringBuilder keyBuilder = new StringBuilder();
        while (token.hasMoreTokens()) {
        	
        	keyBuilder.append(token.nextToken());
        	keyBuilder.append("@");
        	keyBuilder.append(fileName);
			words.set(keyBuilder.toString());
			context.write(words,one);
			keyBuilder.setLength(0);
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
