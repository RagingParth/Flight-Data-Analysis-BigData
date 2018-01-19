import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.LongWritable.Comparator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class FlightAnalysisDriver extends Configured implements Tool
{
	public int run(String[] arg0) throws Exception
	{
		try
		{
			FileSystem fs = FileSystem.get(getConf());

			Job job = Job.getInstance(getConf());
			job.setJarByClass(getClass());

			FileInputFormat.setInputPaths(job, new Path(arg0[0]));

			job.setMapperClass(CancellationMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(LongWritable.class);

			job.setReducerClass(CancellationReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(LongWritable.class);

			FileOutputFormat.setOutputPath(job, new Path(arg0[1] + "_FlightCancellation"));

			if (fs.exists(new Path(arg0[1] + "_FlightCancellation")))
				fs.delete(new Path(arg0[1] + "_FlightCancellation"), true);

			job.waitForCompletion(true);



			Job job2 = Job.getInstance(getConf());
			job2.setJarByClass(getClass());

			FileInputFormat.setInputPaths(job2, new Path(arg0[0]));

			job2.setMapperClass(TaxiInMapper.class);
			job2.setMapOutputKeyClass(Text.class);
			job2.setMapOutputValueClass(IntWritable.class);

			job2.setReducerClass(TaxiReducer.class);
			job2.setOutputKeyClass(Text.class);
			job2.setOutputValueClass(DoubleWritable.class);

			FileOutputFormat.setOutputPath(job2, new Path(arg0[1] + "_TaxiIn"));

			if (fs.exists(new Path(arg0[1] + "_TaxiIn")))
				fs.delete(new Path(arg0[1] + "_TaxiIn"), true);

			job2.waitForCompletion(true);



			Job job3 = Job.getInstance(getConf());
			job3.setJarByClass(getClass());

			FileInputFormat.setInputPaths(job3, new Path(arg0[0]));

			job3.setMapperClass(TaxiOutMapper.class);
			job3.setMapOutputKeyClass(Text.class);
			job3.setMapOutputValueClass(IntWritable.class);

			job3.setReducerClass(TaxiReducer.class);
			job3.setOutputKeyClass(Text.class);
			job3.setOutputValueClass(DoubleWritable.class);

			FileOutputFormat.setOutputPath(job3, new Path(arg0[1] + "_TaxiOut"));

			if (fs.exists(new Path(arg0[1] + "_TaxiOut")))
				fs.delete(new Path(arg0[1] + "_TaxiOut"), true);

			job3.waitForCompletion(true);




			Job job8 = Job.getInstance(getConf());
			job8.setJarByClass(getClass());

			job8.setMapperClass(OnScheduleMapper.class);
			job8.setMapOutputKeyClass(Text.class);
			job8.setMapOutputValueClass(LongWritable.class);

			job8.setSortComparatorClass(StringComparator.class);

			job8.setReducerClass(OnScheduleReducer.class);
			job8.setOutputKeyClass(Text.class);
			job8.setOutputValueClass(DoubleWritable.class);

			FileInputFormat.setInputPaths(job8, new Path(arg0[0]));

			if (fs.exists(new Path(arg0[1] + "_OnSchedule")))
				fs.delete(new Path(arg0[1] + "_OnSchedule"), true);

			FileOutputFormat.setOutputPath(job8, new Path(arg0[1] + "_OnSchedule"));

			return job8.waitForCompletion(true) ? 0 : 1;
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}

		return 0;
	}

	public static void main(String[] args) throws Exception
	{
		System.exit(ToolRunner.run(new FlightAnalysisDriver(), args));
	}

	public static class StringComparator extends WritableComparator
	{

		public StringComparator()
		{
			super(Text.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
		{

			try
			{
				String v1 = Text.decode(b1, s1, l1);
				String v2 = Text.decode(b2, s2, l2);

				return v1.compareTo(v2);
			}
			catch (Exception e)
			{
				throw new IllegalArgumentException(e);
			}
		}
	}
}
