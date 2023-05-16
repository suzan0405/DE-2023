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

public class UBERStudent20191011
{
	public static class UBERStudent20191011Mapper extends Mapper<Object, Text, Text, Text>{
		private Text one_key = new Text();
		private Text one_value = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			String[] token = s.split(",");
			
			StringTokenizer itr = new StringTokenizer(token[1], "/");
			int month = Integer.parseInt(itr.nextToken());
			int day = Integer.parseInt(itr.nextToken());
			int year = Integer.parseInt(itr.nextToken());
			
			LocalDate date = LocalDate.of(year, month, day);
			DayOfWeek dayOfWeek = date.getDayOfWeek();
			int dayOfWeekNumber = dayOfWeek.getValue();
			
			String week = "";
			switch (dayOfWeekNumber) {
				case 1:
					week = "MON";
					break;
				case 2:
					week = "TUE";
					break;
				case 3:
					week = "WED";
					break;
				case 4:
					week = "THR";
					break;
				case 5:
					week = "FRI";
					break;
				case 6:
					week = "SAT";
					break;
				default:
					week = "SUN";
					break;
			}
			one_key.set(token[0] + "," + week);
			one_value.set(token[2] + "," + token[3]);
			context.write(one_key, one_value);
		}	
	}
	
	public static class UBERStudent20191011 extends Reducer<Text,Text,Text,Text> {
//		private Text result = new Text();
//		
		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			for (Text val : values) {
				result.set(val);
				context.write(key, result);
			}
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
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
