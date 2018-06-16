import java.awt.print.Printable;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class InValidRecordsMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	private Text Comment;
	//overriding the map method from mapper interface
	@Override
	public void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException 
	{
		String rowDetails = value.toString();
		String[] parts = rowDetails.split("\\|");
		
		if (parts[0].equals("NA")) 
		{
			Comment = new Text(" - N/A Company Name of the product " + new Text(parts[1]));
			context.write(new Text(parts[0]), Comment);
		}
		else
		if (parts[1].equals("NA")) 
		{
			Comment = new Text(" - N/A  Product Name of Company  " + new Text(parts[0]));
			context.write(new Text(parts[1]), Comment);
		}
	}

}
