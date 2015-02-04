import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;


public class InvertedIndex {
		
		public static void main(String[] args) throws IOException,InterruptedException,ClassNotFoundException {
			
			Configuration conf = new Configuration();
			
			String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

                        if (otherArgs.length != 2) {
                                System.err.println("Usage: wordcount <in> <out>");
                                System.exit(2);
                        }

			Job job = new Job(conf,"InvertedIndex");
			job.setJarByClass(InvertedIndex.class);

			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			// If the map output key/ value types differ from the input types, you must tell Hadoop what they are. 
			// In this case, your map will output each word, and file as the key/value pairs and both are text objects
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

//			Path in = new Path(args[0]);
//			Path  outputPath = new Path(args[1]);
			// Set the HDFS input files for your job. Hadoop expects multiple input files to be separated with commas
			//String [] citation = input.toString().split(",");

//			FileInputFormat.setInputPaths(job, input);
			// Set the output directory for the job
//			FileOutputFormat.setOutputPath(job, outputPath);
			// Delete existing HDFS output directory if it exists, if you dont do this and the directory already exist then the job will fail
			Path  outputPath = new Path(otherArgs[1]);

                        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
                        FileOutputFormat.setOutputPath(job, outputPath);
			outputPath.getFileSystem(conf).delete(outputPath, true);
			// Tell the JobTracker to run the job and block until the job has completed.
			//job.waitForcompletion(true);
			System.exit(job.waitForCompletion(true) ? 0 : 1);
			
		} // Driver class ends here
		
	
	public static class Map extends Mapper <LongWritable,Text,Text,Text> {
		
		private Text documentId; // A Text object to store the document ID (filename) for your input
		private Text word = new Text(); // To cut down on object creation, you create 

		
		/**  this method is called at the start of the map and prior to the map method being called we use this to store input filename for this map */
		@Override 
		protected void setup(Context context) {
			// Extract the filename from the context
			String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
			documentId = new Text(filename);
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// Your value contains an entire line from your file. You tokenize the line using StringUtils (which is way faster than usng String.split )
			for(String token : value.toString().split(",")) {
				word.set(token);
				context.write(word,documentId);
			}
		}
	}



	/** The goal of the reducer is to create an output line for each word, and list of the document IDs in which the word appears*/

	public static class Reduce extends Reducer <Text, Text,Text,Text > {
		private Text docIds = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
				HashSet <Text> uniqueDocIds = new HashSet<Text>();
				// Iterate over all the DocumentIDs for the key
				String csv = "";
				for(Text docId : values) {
						/* ADD the document ID to your set , the reason you create a new Text object is that MapReduce reuses the Text object when
						   Iterating over the values, which mean you want to create a new copy */
						uniqueDocIds.add(new Text(docId));
						if (csv.length() > 0) csv +=",";
							csv += docId.toString();
				}
				/*
				String csv = "";
                                for(Text val:values){
                                if (csv.length() > 0) csv +=",";
                                         csv += val.toString();
                               } */
	
								
				//docIds.set(new Text(StringUtils.join(uniqueDocIds,",")));
				context.write(key,new Text(csv));
			}
	}
}
	
