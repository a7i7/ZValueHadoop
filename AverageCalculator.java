import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import java.math.*;

public class AverageCalculator {

	private static final int TOTAL_COMBINERS = 10;
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key,Text value, OutputCollector<IntWritable,Text> output,Reporter reporter) throws IOException
		{
			DoubleWritable dataVal = new DoubleWritable(new Double(value.toString()));
			int currentKey = (int)(Math.random()*TOTAL_COMBINERS);
			output.collect(new IntWritable(currentKey),new Text(value.toString()+","+1));
			// java.util.logging.Logger.getLogger(AverageCalculator.class.getName()).log(java.util.logging.Level.SEVERE,newKey+" "+value.toString(),"");

		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException
		{

			BigDecimal currentAverage = new BigDecimal("0.0");
			BigDecimal currentCount = new BigDecimal("0.0");
			while(values.hasNext())
			{
				try
				{

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
		System.out.println(args.length);
		if(false && args.length<2)
		{

			System.out.println("hadoop jar <bla,bla>  inputFilePath outputFilePath");

			System.exit(1);
		}
		
		String inputFilePath = args[0];
		String outputFilePath = args[1];
		// String inputFilePath = "data3.in";
		// String outputFilePath = "out";

		JobConf conf = new JobConf(AverageCalculator.class);
		conf.setJobName("Average");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		// conf.setNumMapTasks(5);
		// conf.setNumReduceTasks(5);

		FileInputFormat.setInputPaths(conf,new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf,new Path(outputFilePath));
		JobClient.runJob(conf);
		return;
	}
}
