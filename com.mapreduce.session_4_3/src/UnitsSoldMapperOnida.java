import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class UnitsSoldMapperOnida extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException , InterruptedException
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		
		if(!parts[0].equalsIgnoreCase("NA"))
		{		
			if(parts[0].equalsIgnoreCase("ONIDA"))
			{
				context.write(new Text(parts[3]), new IntWritable(1));
			}
		}
	}
}
