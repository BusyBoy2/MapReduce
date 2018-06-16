import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
public class InValidRecords  {
	public static void main (String[] args) throws Exception{
		
		///check if valid set of arguments is passed
		if (args.length < 2)
		{
			System.err.println("Usage: InValidRecords <input path> <output path>");
			System.exit(-1);
		}
		
		//set configuration
		Configuration conf = new Configuration();
		//create a new job with the above configuration
		Job job  = new Job(conf,"InValidRecords");
		//set jar class
		job.setJarByClass(InValidRecords.class);
		
		//Creating Filesystem object with the configuration	
		FileSystem fs = FileSystem.get(conf);
		//Check if output path (args[1])exist or not
		if(fs.exists(new Path(args[1]))){
		//If exist delete the output path
		   fs.delete(new Path(args[1]),true);
		}
		
		//check for input argument 
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		//set reducer task to ZERO
		job.setNumReduceTasks(0);
		
		//set mapper class
		job.setMapperClass(InValidRecordsMapper.class);
		
		//Set the input format class
		job.setInputFormatClass(TextInputFormat.class);
		//set the output format class
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//set the the output key & value classes 	
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    //execute the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
	}
	

}
