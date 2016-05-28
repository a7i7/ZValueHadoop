import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import java.util.*;
import java.io.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import java.math.*;

public class ZValueTest {

	private static final int TOTAL_COMBINERS = 30;
	private static int currentKey = 0;
	public static Double mean = 0.0;
	public static Double stddev = 1.0;
	public static Double thresholdLimit = 0.5; 
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text>
	{
		public void map(LongWritable key,Text value, OutputCollector<IntWritable,Text> output,Reporter reporter) throws IOException
		{
			DoubleWritable dataVal = new DoubleWritable(new Double(value.toString()));
			int currentKey = (int)(Math.random()*TOTAL_COMBINERS);
			BigDecimal deviation = new BigDecimal(value.toString());
			deviation = deviation.subtract(new BigDecimal(mean.toString()));
			deviation = deviation.abs();
			deviation = deviation.divide(new BigDecimal(stddev),5,RoundingMode.HALF_UP);
			if(deviation.compareTo(new BigDecimal(thresholdLimit))>0)
				output.collect(new IntWritable(currentKey),new Text(value.toString()+","+deviation.toString()));
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable, Text>
	{
		public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<IntWritable,Text> output, Reporter reporter) throws IOException
		{
			String ans="";
			while(values.hasNext()) 
				ans+=values.next().toString()+"::";
			output.collect(new IntWritable(1),new Text(ans));
		}
	}
	

	public static void main(String args[]) throws Exception
	{
		JobConf conf = new JobConf(ZValueTest.class);

		if(args.length<3)
		{
			System.out.println("hadoop jar <bla,bla>  mean stddev inputFilePath outputFilePath");
			System.exit(1);
		}
		
		mean = new Double(args[0]);
		stddev = new Double(args[1]);
		String inputFilePath = args[2];
		String outputFilePath = args[3];


		conf.setJobName("ZValueTest");

		conf.setOutputKeyClass(IntWritable.class);
		conf.setOutputValueClass(Text.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf,new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf,new Path(outputFilePath));
		JobClient.runJob(conf);
		return;
	}
}
