import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UnitsSoldMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		
		if(!parts[0].equalsIgnoreCase("NA"))
		{
			if(!parts[1].equalsIgnoreCase("NA"))
			{
				context.write(new Text(parts[0]), new IntWritable(1));
			}						
		}
	}

}
