import java.io.*;
import java.util.*;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class OnScheduleReducer<KEY> extends Reducer<Text, LongWritable, Text, DoubleWritable>
{
	Map<String, Long> hm = new HashMap<String, Long>();

	double max1 = 0L, max2 = 0L, max3 = 0L;
	double min1 = Double.MAX_VALUE, min2 = Double.MAX_VALUE, min3 = Double.MAX_VALUE;
	Text uniqueCarrierMax1 = new Text();
	Text uniqueCarrierMax2 = new Text();
	Text uniqueCarrierMax3 = new Text();
	Text uniqueCarrierMin1 = new Text();
	Text uniqueCarrierMin2 = new Text();
	Text uniqueCarrierMin3 = new Text();

	public void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException
	{
		try
		{
			if(key.toString().contains("All: "))
			{
				String uniqueCarrier = key.toString().replace("All: ", "");
				long sumTotal = 0;

				for (LongWritable val : values)
					sumTotal++;

				hm.put(uniqueCarrier, sumTotal);
			}
			else if(key.toString().contains("Delayed: "))
			{
				long sum = 0;
				String uniqueCarrier = key.toString().replace("Delayed: ", "");
				long sumTotal = hm.get(uniqueCarrier);

				for (LongWritable val : values)
					sum++;

				double probability = sum / (double) sumTotal;

				if (probability > max1)
					exchangeMax1(probability, uniqueCarrier);
				else if (probability > max2)
					exchangeMax2(probability, uniqueCarrier);
				else if (probability > max3)
					exchangeMax3(probability, uniqueCarrier);

				if (probability < min1)
					exchangeMin1(probability, uniqueCarrier);
				else if (probability < min2)
					exchangeMin2(probability, uniqueCarrier);
				else if (probability < min3)
					exchangeMin3(probability, uniqueCarrier);
			}
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}

	public void exchangeMax1(double probability, String uniqueCarrier)
	{
		max3 = max2;
		uniqueCarrierMax3.set(uniqueCarrierMax2.toString());
		max2 = max1;
		uniqueCarrierMax2.set(uniqueCarrierMax1.toString());
		max1 = probability;
		uniqueCarrierMax1.set(uniqueCarrier);
	}

	public void exchangeMax2(double probability, String uniqueCarrier)
	{
		max3 = max2;
		uniqueCarrierMax3.set(uniqueCarrierMax2.toString());
		max2 = probability;
		uniqueCarrierMax2.set(uniqueCarrier);
	}

	public void exchangeMax3(double probability, String uniqueCarrier)
	{
		max3 = probability;
		uniqueCarrierMax3.set(uniqueCarrier);
	}

	public void exchangeMin1(double probability, String uniqueCarrier)
	{
		min3 = min2;
		uniqueCarrierMin3.set(uniqueCarrierMin2.toString());
		min2 = min1;
		uniqueCarrierMin2.set(uniqueCarrierMin1.toString());
		min1 = probability;
		uniqueCarrierMin1.set(uniqueCarrier);
	}

	public void exchangeMin2(double probability, String uniqueCarrier)
	{
		min3 = min2;
		uniqueCarrierMin3.set(uniqueCarrierMin2.toString());
		min2 = probability;
		uniqueCarrierMin2.set(uniqueCarrier);
	}

	public void exchangeMin3(double probability, String uniqueCarrier)
	{
		min3 = probability;
		uniqueCarrierMin3.set(uniqueCarrier);
	}

	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException
	{
		context.write(new Text("The following are the Flights with Highest probability for being on schedule"), null);
		context.write(uniqueCarrierMax1, new DoubleWritable(max1));
		context.write(uniqueCarrierMax2, new DoubleWritable(max2));
		context.write(uniqueCarrierMax3, new DoubleWritable(max3));
		context.write(null, null);
		context.write(new Text("The following are the Flights with Lowest probability for being on schedule"), null);
		context.write(uniqueCarrierMin1, new DoubleWritable(min1));
		context.write(uniqueCarrierMin2, new DoubleWritable(min2));
		context.write(uniqueCarrierMin3, new DoubleWritable(min3));
	}
}