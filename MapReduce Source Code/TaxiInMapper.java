import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TaxiInMapper extends Mapper<Object, Text, Text, IntWritable>
{
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException
	{
		String[] tokens = value.toString().split(",");
		String dest = tokens[17];
		String taxiIn = tokens[19];

		if(!dest.equals("NA") && !dest.equals("Dest") && !taxiIn.equals("NA"))
                	context.write(new Text(dest), new IntWritable(Integer.parseInt(taxiIn)));
      	}
}