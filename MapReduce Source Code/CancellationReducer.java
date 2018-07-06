import java.io.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class CancellationReducer<KEY> extends Reducer<Text, LongWritable, Text, LongWritable>
{
	long max = 0L;
	Text maxCancellationCode = new Text();

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		long sum = 0;

		for (LongWritable val : values)
			sum += val.get();

		if (sum >= max)
		{
			maxCancellationCode.set(key);
			max = sum;
		}
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		context.write(maxCancellationCode, new LongWritable(max));
	}
}