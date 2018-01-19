import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class CancellationMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException
	{
		String[] tokens = record.toString().split(",");

		if(tokens[0] != null && !tokens[22].equals("CancellationCode") && !tokens[22].equals("NA"))
		{
			String cancellationCode = tokens[22];

			if (cancellationCode != null && !cancellationCode.trim().equals(""))
				context.write(new Text(cancellationCode), new LongWritable(1));
		}
	}
}