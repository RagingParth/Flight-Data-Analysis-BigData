import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiOutMapper extends Mapper<Object, Text, Text, IntWritable>
{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split(",");
		String origin = tokens[16];
		String taxiOut = tokens[20];

		if(!origin.equals("NA") && !origin.equals("Origin") && !taxiOut.equals("NA"))
                	context.write(new Text(origin), new IntWritable(Integer.parseInt(taxiOut)));
      	}
}