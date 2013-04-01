package testing;
 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
 
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * WordFrequenceInDocument Creates the index of the words in documents,
 * mapping each of them to their frequency.
 */
public class WordFrequenceInDocument extends Configured implements Tool {
 
    // where to put the data in hdfs when we're done
 //   private static final String OUTPUT_PATH = "1-word-freq";
 
    // where to read the data from.
 //   private static final String INPUT_PATH = "input";
 



    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
 	
	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
                if(otherArgs.length !=2)
                {
                        System.err.println("Input format error");
                        System.exit(2);
                }


        Job job = new Job(conf, "Word Frequence In Document");
 
        job.setJarByClass(WordFrequenceInDocument.class);
        job.setMapperClass(WordCountsForDocsMapper.class);
//        job.setReducerClass(WordFrequenceInDocReducer.class);
//        job.setCombinerClass(WordFrequenceInDocReducer.class);
 
//        job.setReducerClass(WordFrequenceInDocReducer.class);

	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WordFrequenceInDocument(), args);
        System.exit(res);
    }
}
