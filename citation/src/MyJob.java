import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.GenericOptionsParser;

public class MyJob {
		
		public static class MapClass extends Mapper <LongWritable ,Text,Text,Text > {
			public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
				
					String [] citation = value.toString().split(",");
					context.write(new Text(citation[1]), new Text(citation[0]));
		
				}
			}


		public static class Reduce extends Reducer<Text,Text,Text,Text> {
				
			public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
					String csv = "";
					for(Text val:values){
							if (csv.length() > 0) csv +=",";
							csv += val.toString();
					}
				context.write(key, new Text(csv));
			}
		}

		public static void main(String[] args) throws Exception {
			Configuration conf = new Configuration();
			String [] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

			if (otherArgs.length != 2) {
     				System.err.println("Usage: wordcount <in> <out>");
      				System.exit(2);
    			}

			Job job = new Job(conf,"MyJob");
			job.setJarByClass(MyJob.class);
	

		//	Path in = new Path(args[0]);
		//	Path out= new Path(args[1]);

		//	FileInputFormat.setInputPaths(job, in);
		//	FileOutputFormat.setOutputPath(job, out);


			job.setMapperClass(MapClass.class);
			job.setReducerClass(Reduce.class);

			//job.setInputFormatClass(TextInputFormat.class); 

			//job.setOutputFormatClass(TextOutputFormat.class);
			job.setOutputKeyClass(Text.class);

			job.setOutputValueClass(Text.class);

			FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    			FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

			 System.exit(job.waitForCompletion(true) ? 0 : 1);	


		}

}
	


	
