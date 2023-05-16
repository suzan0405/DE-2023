import java.io.IOException;
import java.time.DayOfWeek;
import java.time.LocalDate;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;
// import org.apache.hadoop.fs.FSDataInputStream;
// import org.apache.hadoop.fs.FSDataOutputStream;

public class UBERStudent20191011
{
	public static String dayOfWeek(int day){
		switch (day) {
				case 1:
					return "MON";
				case 2:
					return "TUE";
				case 3:
					return "WED";
				case 4:
					return "THR";
				case 5:
					return "FRI";
				case 6:
					return "SAT";
				default:
					return "SUN";
			}
	}
	
	public static class UBERStudent20191011Mapper extends Mapper<Object, Text, Text, Text>{
		private Text word = new Text();
		private Text value = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString(), ",");
			String baseNum = itr.nextToken().trim();
			
			String date = itr.nextToken().trim();
			String[] d = date.split("/");
			int month = d[0];
			int day = d[1];
			int year = d[2];
			
			int vehicle = Integer.parseInt(itr.nextToken().trim());
			int trip = Integer.parseInt(itr.nextToken().trim());
			
			LocalDate date = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = date.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
			
			word.set(baseNum + "," + dayOfWeekNumber);
			value.set(trip + "," + vehicle);
			context.write(word, value);
		}	
	}
	
	public static class UBERStudent20191011Reducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			int vehiclesSum = 0;
			int tripsSum = 0;
			
			StringTokenizer itr = new StringTokenizer(key.toString(), ",");
			String baseNum = itr.nextToken().trim();
			int day = Integer.parseInt(itr.nextToken().trim());
			String dayOfWeek = dayOfWeek(day);
			key.set(baseNum + "," + dayOfWeek);
			
			for (Text val : values) {
				itr = new StringTokenizer(val.toString(), ",");
				int trip = Integer.parseInt(itr.nextToken().trim());
				int vehicle = Integer.parseInt(itr.nextToken().trim());
				
				vehiclesSum += vehicle;
				tripsSum += trip;
			}
			result.set(tripsSum + "," + vehiclesum);
			context.write(key, result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "uberstudent20191011");
		job.setJarByClass(UBERStudent20191011.class);
		job.setMapperClass(UBERStudent20191011Mapper.class);
		job.setCombinerClass(UBERStudent20191011Reducer.class);
		job.setReducerClass(UBERStudent20191011Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
// 		job.setInputFormatClass(TextInputFormat.class);
// 		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}