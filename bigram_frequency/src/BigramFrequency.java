package org.rashed.hadoop.examples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BigramFrequency extends Configured implements Tool{

		public static class BigramMapper extends Mapper <LongWritable,Text,Text, IntWritable> {

		private final static IntWritable count = new IntWritable(1);
		private Text bigram = new Text();

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String strText = value.toString().replaceAll("\\p{Punct}+"," ");
		StringTokenizer tokenizer = new StringTokenizer(strText);

		// Using a java List  data structure

		List<String> tokens = new ArrayList<String>();
		while(tokenizer.hasMoreTokens()){
			String currentToken = tokenizer.nextToken().toLowerCase().replaceAll("\\p{Punct}+"," ");
			if(currentToken.length() >1){
				tokens.add(currentToken);
			}
		}
	
		//
		for(int i=0; i<tokens.size()-1;i++) {
			bigram.set(tokens.get(i)+" "+tokens.get(i+1));
			context.write(bigram,count);
		}

	} // Map function Ends here
    } // map classs ends here 

	// Reducer 

	public static class BigramReducer extends Reducer <Text ,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException  {

			int sumBigram =0;
			for (IntWritable val : values) {
				sumBigram += val.get();
			}

			context.write(key, new IntWritable(sumBigram));
		}
	}
			




	// write the driver here

	public int run(String[] args) throws Exception{

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf,args).getRemainingArgs();

		if(otherArgs.length !=2){
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);	
		}
		Job job = new Job(conf, "Bigram");
		
		
		job.setJarByClass(BigramFrequency.class);
		job.setMapperClass(BigramFrequency.BigramMapper.class);
		job.setCombinerClass(BigramFrequency.BigramReducer.class);
		job.setReducerClass(BigramFrequency.BigramReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);


		
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job,new Path(otherArgs[1]));
		return 	job.waitForCompletion(true) ? 1: 0;

	}//Main method ends here
		
public static void main(String[] args) throws Exception{
		
	int rs=	ToolRunner.run(new Configuration(),new BigramFrequency(),args);
	System.exit(rs);

	}

}
			
		
		
	


















			
					

















