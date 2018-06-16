import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalUnitsSoldforOnida {
public static void main (String[] args) throws Exception {
		
		//check if valid set of arguments is passed
		if(args.length < 2)
		{
			System.err.println("Usage: TotalUnitsSoldForOnida <input path> <output path>");
			System.exit(-1);
		}
		
		//get configuration details and create a job
		Configuration conf = new Configuration();
		Job job = new Job(conf,"TotalUnitsSoldForOnia");
		//set jar class
		job.setJarByClass(TotalUnitsSoldforOnida.class);

		//Creating Filesystem object with the configuration	
		FileSystem fs = FileSystem.get(conf);
		//Check if output path (args[1])exist or not
		if(fs.exists(new Path(args[1]))){
		//If exist delete the output path
		fs.delete(new Path(args[1]),true);
		}

		//set input path and output path
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//set mapper class & Reducer class
		job.setNumReduceTasks(1);
		job.setMapperClass(UnitsSoldMapperOnida.class);
		job.setReducerClass(UnitsSoldReducerOnida.class);
		
		//Set the input format class & Output format
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//set the the output key & value classes 
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		   //execute the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
