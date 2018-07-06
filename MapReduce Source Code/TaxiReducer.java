import java.io.*;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TaxiReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
{
	double max1 = 0, max2 = 0, max3 = 0;
	double min1 = Double.MAX_VALUE, min2 = Double.MAX_VALUE, min3 = Double.MAX_VALUE;
	Text taxiMax1 = new Text();
	Text taxiMax2 = new Text();
	Text taxiMax3 = new Text();
	Text taxiMin1 = new Text();
	Text taxiMin2 = new Text();
	Text taxiMin3 = new Text();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
	{
		int count = 0, taxiSum = 0;

		for (IntWritable val : values)
		{
              		count += 1;
              		taxiSum += val.get();
          	}

          	double avg = (double)taxiSum / (double)count;

		if (avg > max1)
			exchangeMax1(avg, key.toString());
		else if (avg > max2)
			exchangeMax2(avg, key.toString());
		else if (avg > max3)
			exchangeMax3(avg, key.toString());

		if (avg < min1)
			exchangeMin1(avg, key.toString());
		else if (avg < min2)
			exchangeMin2(avg, key.toString());
		else if (avg < min3)
			exchangeMin3(avg, key.toString());

          	//list.add(new AvgTaxiTime(avg, key.toString()));
	}

	public void exchangeMax1(double avg, String key)
	{
		max3 = max2;
		taxiMax3.set(taxiMax2.toString());
		max2 = max1;
		taxiMax2.set(taxiMax1.toString());
		max1 = avg;
		taxiMax1.set(key.toString());
	}

	public void exchangeMax2(double avg, String key)
	{
		max3 = max2;
		taxiMax3.set(taxiMax2.toString());
		max2 = avg;
		taxiMax2.set(key.toString());
	}

	public void exchangeMax3(double avg, String key)
	{
		max3 = avg;
		taxiMax3.set(key.toString());
	}

	public void exchangeMin1(double avg, String key)
	{
		min3 = min2;
		taxiMin3.set(taxiMin2.toString());
		min2 = min1;
		taxiMin2.set(taxiMin1.toString());
		min1 = avg;
		taxiMin1.set(key.toString());
	}

	public void exchangeMin2(double avg, String key)
	{
		min3 = min2;
		taxiMin3.set(taxiMin2.toString());
		min2 = avg;
		taxiMin2.set(key.toString());
	}

	public void exchangeMin3(double avg, String key)
	{
		min3 = avg;
		taxiMin3.set(key.toString());
	}

	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		context.write(new Text("The following are the top 3 Results"), null);
		context.write(taxiMax1, new DoubleWritable(max1));
		context.write(taxiMax2, new DoubleWritable(max2));
		context.write(taxiMax3, new DoubleWritable(max3));
		context.write(null, null);
		context.write(new Text("The following are the bottom 3 Results"), null);
		context.write(taxiMin1, new DoubleWritable(min1));
		context.write(taxiMin2, new DoubleWritable(min2));
		context.write(taxiMin3, new DoubleWritable(min3));
	}
}