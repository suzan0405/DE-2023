import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class YouTubeStudent20191011 {

    public static class YouTube {
        public String category;
        public double rating;

        public YouTube(String category, double rating) {
            this.category = category;
            this.rating = rating;
        }

        public void setCategory(String category) {
            this.category = category;
        }
        public String getCategory() { return this.category; }
        public void setRating(double rating) {
            this.rating = rating;
        }
        public double getRating() { return this.rating; }
    }

    public static class YouTubeComparator implements Comparator<YouTube> {
        @Override
        public int compare(YouTube o1, YouTube o2) {
            if (o1.rating > o2.rating) {
                return 1;
            } else if (o1.rating == o2.rating) {
                return 0;
            } else {
                return -1;
            }
        }
    }

    public static void insertYoutube(PriorityQueue q, String category, double rating, int topK) {
        YouTube youTube_head = (YouTube) q.peek();
        if (q.size() < topK || youTube_head.rating < rating) {
            YouTube youTube = new YouTube(category, rating);
            q.add(youTube);
            if(q.size() > topK) q.remove();
        }
    }

    public static class YouTubeStudent20191011Mapper extends Mapper<Object, Text, Text, DoubleWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split("\\|");
            String category = data[3]; 
            double rating = Double.parseDouble(data[6]); 

            context.write(new Text(category), new DoubleWritable(rating));
        }
    }

    public static class YouTubeStudent20191011Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private PriorityQueue<YouTube> queue;
        private Comparator<YouTube> comp = new YouTubeComparator();
        private int topK;
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
		
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            double average = sum / (double) count;
            insertYoutube(queue, key.toString(), average, topK);
        }

        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            topK = conf.getInt("topK", -1);
            queue = new PriorityQueue<YouTube>(topK, comp);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while( queue.size() != 0 ) {
                YouTube youTube = (YouTube) queue.remove();
                context.write( new Text( youTube.getCategory() ), new DoubleWritable(youTube.getRating()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        // if (otherArgs.length != 3) {
        //     System.err.println("Usage: YouTubeStudent20191011 <in> <out>");
        //     System.exit(2);
        // }

        conf.setInt("topK", Integer.parseInt(otherArgs[2]));
        Job job = new Job(conf, "youtubestudent20191011");
        job.setJarByClass(YouTubeStudent20191011.class);
        job.setMapperClass(YouTubeStudent20191011Mapper.class);
        job.setReducerClass(YouTubeStudent20191011Reducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        FileSystem.get(job.getConfiguration()).delete( new Path(otherArgs[1]), true);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
