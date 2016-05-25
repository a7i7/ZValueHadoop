import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import java.math.*;

public class VarianceCalculator {

	private static final int TOTAL_COMBINERS = 30;
	private static int currentKey = 0;
	public static Double mean = 3.5;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key,Text value, OutputCollector<IntWritable,Text> output,Reporter reporter) throws IOException
		{
			DoubleWritable dataVal = new DoubleWritable(new Double(value.toString()));
			int currentKey = (int)(Math.random()*TOTAL_COMBINERS);
			BigDecimal differenceFromMean = new BigDecimal(value.toString());
			differenceFromMean = differenceFromMean.subtract(new BigDecimal(mean.toString()));
			differenceFromMean = differenceFromMean.multiply(differenceFromMean);
			output.collect(new IntWritable(currentKey),new Text(differenceFromMean.toString()+","+1));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException
		{
			if(!true)
			{
				String ans = "";
				while(values.hasNext())
					ans+=values.next()+",";
				output.collect(key,new Text(ans.toString()));
				return;
			}
			BigDecimal currentAverage = new BigDecimal("0.0");
			BigDecimal currentCount = new BigDecimal("0.0");
			while(values.hasNext())
			{
				try{
				Text val = values.next();

					BigDecimal newAvg = new BigDecimal(val.toString().split(",")[0]);
					BigDecimal newCount = new BigDecimal(val.toString().split(",")[1]);
					BigDecimal temp = currentAverage.multiply(currentCount);
					temp = temp.add(newAvg.multiply(newCount));
					currentCount = currentCount.add(newCount);
					temp = temp.divide(currentCount,5,RoundingMode.HALF_UP);
					currentAverage = temp;
				}
				catch(Exception e)
				{
					java.util.logging.Logger.getLogger(AverageCalculator.class.getName()).log(java.util.logging.Level.SEVERE,e.toString(),e.toString());
				}
			}
			output.collect(new IntWritable(1),new Text(currentAverage.toString()+","+currentCount.toString()));
		}
	}
	

	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(VarianceCalculator.class);
		conf.setJobName("Variance");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf,new Path("so_many_numbers.txt.bak"));
		FileOutputFormat.setOutputPath(conf,new Path("variance_ans"));
		JobClient.runJob(conf);
		return;
	}
}
