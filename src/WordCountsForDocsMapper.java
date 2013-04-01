package testing;

import java.util.StringTokenizer;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class WordCountsForDocsMapper extends Mapper<LongWritable,Text,Text,Text> {

		
	public WordCountsForDocsMapper(){
	}

	public void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {

		// Wring reducer)

		
		String [] wordAndDocCounter = value.toString().split("\t");
	//	String [] wordAndDoc = wordAndDocCounter[1].split(",");

		StringTokenizer st = new StringTokenizer(wordAndDocCounter[1],",");
		int sumWords = 0;
		while(st.hasMoreTokens()){
			sumWords +=1;
		}
		String word = wordAndDocCounter[1]+" "+sumWords;
		context.write(new Text(wordAndDocCounter[0]), new Text(word));//new Text(word));

	}
}
	


	
