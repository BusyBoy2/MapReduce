import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TotalUnitsSold {
	public static void main (String[] args) throws Exception {
		
		//check if valid set of arguments is passed
		if(args.length < 2)
		{
			System.err.println("Usage: TotalUnitsSold <input path> <output path>");
			System.exit(-1);
		}
		
		//get configuration details and create a job
		Configuration conf = new Configuration();
		Job job = new Job(conf,"TotalUnitsSold");
		//set jar class
		job.setJarByClass(TotalUnitsSold.class);
		

		//Creating Filesystem object with the configuration	
		FileSystem fs = FileSystem.get(conf);
		//Check if output path (args[1])exist or not
		if(fs.exists(new Path(args[1]))){
		//If exist delete the output path
		   fs.delete(new Path(args[1]),true);
		}
		
		//Set input path and output path
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		Path outputPath = new Path(args[1]);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		//Set Mapper & Reduces & formats of output n Input
		job.setNumReduceTasks(1);
		job.setMapperClass(UnitsSoldMapper.class);
		job.setReducerClass(UnitsSoldReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		   //execute the job
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
