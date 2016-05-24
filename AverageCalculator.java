import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import java.math.BigDecimal;
public class AverageCalculator {

	private static final int TOTAL_COMBINERS = 3;

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key,Text value, OutputCollector<IntWritable,Text> output,Reporter reporter) throws IOException
		{
			DoubleWritable dataVal = new DoubleWritable(new Double(value.toString()));
			int newKey = (int)(Math.random()*TOTAL_COMBINERS);
			output.collect(new IntWritable(newKey),new Text(value.toString()+","+1));
			java.util.logging.Logger.getLogger(AverageCalculator.class.getName()).log(java.util.logging.Level.SEVERE,newKey+" "+value.toString(),"");

		}
	}

	// public static class Combine extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	// {
	// 	public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException
	// 	{
	// 		BigDecimal sum = new BigDecimal(0.0);
	// 		int total = 0;
	// 		while(values.hasNext())
	// 		{
	// 			Double next = new Double(values.next().toString());
	// 			sum = sum.add(new BigDecimal(next));
	// 			++total;
	// 		}
	// 		sum = sum.divide(new BigDecimal(total));
	// 		output.collect(new IntWritable(1),new Text(sum+","+total));
	// 	}
	// }

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException
		{
			BigDecimal sum = new BigDecimal("0.0");
			BigDecimal count = new BigDecimal("0.0");
			while(values.hasNext())
			{
				Text val = values.next();

				BigDecimal currentAvg = new BigDecimal(val.toString().split(",")[0]);
				BigDecimal currentCount = new BigDecimal(val.toString().split(",")[1]);
				count = count.add(currentCount);
				sum = sum.add(currentAvg.multiply(currentCount));
			}
			sum = sum.divide(count);
			output.collect(new IntWritable(1),new Text(sum.toString()+","+count.toString()));

		}
	}
	

	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(AverageCalculator.class);
		conf.setJobName("Average");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf,new Path("so_many_numbers.txt"));
		FileOutputFormat.setOutputPath(conf,new Path("ans"));
		JobClient.runJob(conf);
		return;
	}
}