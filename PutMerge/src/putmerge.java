import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;



// This program copies and merges data from local directory to HDFS

public class PutMerge {
	
	/* Configuration class is a special class for holding key/value configuration parameters. 
	* 
	*/
	public static void main(String[] args) throws IOException {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		FileSystem local = FileSystem.getLocal(conf);

		Path inputDir = new Path(args[0]) // Specify the input directory 
		Path hdfsFile = new Path(args[1]) // Specify the output directory

		try {
			FileStatus [] inputFiles = local.listStatus(inputDir); // Get list of local Files
			FSDataOutputStream out = hdfs.create(hdfsFile)	 // Create HDFS output stream
	
			for( int i=0;i < inputFiles.length; i++) {
				System.out.println(inputFiles[i].getPath().getName());
				FSDataInputStream in = local.open(inputFiles[i].getPath());
				
				byte buffer[] = new byte[256]; // Open local input stream
				int bytesRead = 0;
				while ((bytesRead = in.read(buffer)) > 0 ){
					out.write(buffer,0,bytesRead);
				}
				
				in.close();
			}

			out.close()
		}catch(IOException e) {
			e.printStackTree();
		}
	}

}
