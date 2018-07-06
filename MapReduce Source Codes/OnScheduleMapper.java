import java.io.*;
import java.util.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class OnScheduleMapper extends Mapper<LongWritable, Text, Text, LongWritable>
{
	public void map(LongWritable lineOffSet, Text record, Context context) throws IOException, InterruptedException
	{
		String[] strArr = record.toString().split(",");

		if (strArr[0] != null && !strArr[8].equals("UniqueCarrier"))
		{
			String uniqueCarrier = strArr[8];
			String arrDelay = strArr[14];

			if (uniqueCarrier != null && arrDelay != null && !uniqueCarrier.trim().equals("") && !arrDelay.trim().equals(""))
			{
				try
				{
					Integer arrDelayInt = Integer.parseInt(arrDelay);
					context.write(new Text("All: " + uniqueCarrier), new LongWritable(1));

					if (arrDelayInt > 10)
						context.write(new Text("Delayed: " + uniqueCarrier), new LongWritable(1));
				}
				catch (Exception e)
				{
				}
			}
		}
	}
}