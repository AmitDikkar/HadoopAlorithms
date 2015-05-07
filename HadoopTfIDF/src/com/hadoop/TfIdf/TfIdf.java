package com.hadoop.TfIdf;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TfIdf {

	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		generateInputFiles(args[0]);
		// This is the stage-1 configuration
		Configuration conf = new Configuration();
		System.out.println("Created first stage configuration");
		Job job = new Job(conf, "wordcount");
		System.out.println("Created first stage Job");
		job.setJarByClass(TfIdf.class);
		

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setMapperClass(Stage1Mapper.class);
		job.setReducerClass(Stage1Reducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path("tempFiles"));
		FileOutputFormat.setOutputPath(job, new Path("stage-1-output"));

		job.waitForCompletion(true);
		System.out.println("First Stage Completed");
		
		// This is the stage-2 configuration
		Configuration conf2 = new Configuration();
		System.out.println("Created second stage configuration");
		Job job2 = new Job(conf2, "wordcount2");
		System.out.println("Created second stage Job");
		job2.setJarByClass(TfIdf.class);
		
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(Text.class);

		job2.setMapperClass(Stage2Mapper.class);
		job2.setReducerClass(Stage2Reducer.class);

		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job2, new Path("stage-1-output"));
		FileOutputFormat.setOutputPath(job2, new Path("stage-2-output"));

		job2.waitForCompletion(true);
		System.out.println("Second Stage Completed");
		
		// This is the stage-2 configuration
		Configuration conf3 = new Configuration();
		FileSystem fs = FileSystem.get(conf3);
		FileStatus[] userFilesStatusList = fs.listStatus(new Path(args[0]));
        final int numberOfUserInputFiles = userFilesStatusList.length;
        conf3.setInt("numberOfDocsInCorpus", numberOfUserInputFiles);
        
		System.out.println("Creating thrid stage configuration");
		Job job3 = new Job(conf3, "wordcount3");
		System.out.println("Created third stage Job");
		job3.setJarByClass(TfIdf.class);
		
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(Text.class);

		job3.setMapperClass(Stage3Mapper.class);
		job3.setReducerClass(Stage3Reducer.class);

		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job3, new Path("stage-2-output"));
		FileOutputFormat.setOutputPath(job3, new Path("output"));

		job3.waitForCompletion(true);
		System.out.println("Third Stage Completed");
	}

	private static void generateInputFiles(String inputDirectory) throws IOException {
		File folder = new File(inputDirectory);
		File[] listOfFiles = folder.listFiles();
		System.out.println("files: " +listOfFiles[0] + ", " + listOfFiles[1]);
		
		Scanner scanner = new Scanner(new File(listOfFiles[0].toString()));
		Scanner scanner1 = new Scanner(new File(listOfFiles[1].toString()));
		
		new File("tempFiles").mkdir();
		writeToFromFile(scanner);
		writeToFromFile(scanner1);
	}

	private static void writeToFromFile(Scanner scanner) throws IOException {
		List<String> fileLines = new ArrayList<String>();
		int i = 0; int noOfFiles=0;
		
		while (scanner.hasNextLine()) {
			  String line = scanner.nextLine();
			  fileLines.add(line);
			  //System.out.println(line);
			  i++;
			  if(i == 35){
				  noOfFiles++;
				  String fileName = "temp-" + noOfFiles;
				  writeLinesToFile(fileName, fileLines);
				  i=0;
				  fileLines = new ArrayList<String>();
			  }
			}
			noOfFiles++;
			writeLinesToFile("temp-" + noOfFiles , fileLines);
		
	}

	private static void writeLinesToFile(String fileName, List<String> fileLines) throws IOException {
		String path = "tempFiles" + File.separator + fileName;
		System.out.println("number of lines are: " + fileLines.size());
		PrintWriter writer = new PrintWriter(path, "UTF-8");
		for(String line : fileLines){
			writer.println(line);
		}
		writer.close();	
		/*for(String line : fileLines){
		}*/
	}


}
