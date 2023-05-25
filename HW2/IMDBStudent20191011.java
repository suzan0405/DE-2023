import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.GenericOptionsParser;

public class IMDBStudent20191011
{
    public class Movie {
        private String title;
        private String rating;

        public Movie() {
            this.title="";
            this.rating="";
        }

        public Movie(String title, String rating) { 
            this.title = title;
            this.rating = rating;
        }

        public void setTitle(String title) {
            this.title = title;
        }
        public String getTitle() {
            return title;
        }

        public void setRating(String rating) {
            this.rating = rating;
        }
        public String getRating() {
            return rating;
        }
    }

    public static void insert(PriorityQueue queue, String item, String rating, int topN) {
        Movie head = (Movie)queue.peek();

        if (queue.size() < topN || head.getRating().equals(rating)) {
            Movie movie = new Movie(item, rating); 
            queue.add(movie);
            if (queue.size() > topN) {
                queue.remove();
            }
        }
    }

    public static class MovieComparator implements Comparator<Movie> {

        public int compare(Movie x, Movie y) {
            int x_rating = Integer.parseInt(x.getRating());
            int y_rating = Integer.parseInt(y.getRating());

            if (x_rating < y_rating) {
                return -1;
            }
        
            if (x_rating > y_rating) {    
            return 1;
            }

            return 0;
        }
    }

	public static class IMDBStudent20191011Mapper extends Mapper<Object, Text, Text, Text>{
        boolean file_movies = true;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String s = value.toString();
			String[] token = s.split("::");
			int len = token.length;
			StringTokenizer itr = new StringTokenizer(token[len-1], "|");

			Text outputKey = newText();
            Text outputValue = newText();
            String joinKey = "";
            String o_value = "";
			
            if (file_movies) {
                while (itr.hasMoreTokens()) {
                    if (itr.nextToken().trim().toString().equals("Fantasy")){
                        joinKey = token[0];
                        o_value = "M," + token[1];
                        break;
                    }
                }
            }
            else {
                joinKey = token[1];
                o_value = "R," + token[2];
            }
            outputKey.set(joinKey);
            outputValue.set(o_value);
            context.write(outputKey, outputValue);
		}	

        protected void setup(Context context) throws IOException, InterruptedException {
            String filename = ((FileSplit) context.getInputSplit()).getPath().getName();
            if ( filename.indexOf( "movies.dat" ) != -1 ) file_movies = true;
            else file_movies = false;
        }
	}
	
	public static class IMDBStudent20191011Reducer extends Reducer<Text,Text,Text,Text> {
		int topK = 0;
		Comparator<Movie> comparator = new MovieComparator();
        PriorityQueue<Movie> queue = new PriorityQueue<Movie>(topK, comparator);

		public void reduce(Text key, Iterable<Text> values, Context context ) throws IOException, InterruptedException {
			int count = 0;
            int sum = 0;
            String title = "";
            String rating = "";

			for (Text val : values) {
                StringTokenizer itr = new StringTokenizer(val.toString(), ",");
                String head = itr.nextToken().trim().toString();
                String h_value = itr.nextToken().trim().toString();
				if (head.equals("M")) {
                    title = h_value;
                }
                else {
                    sum += Integer.parseInt(h_value);
                    count++;
                }
			}
            Integer avg = sum / count;
			rating = avg.toString();
            insert(queue, title, rating, topK);
		}

        protected void setup(Context context) throws IOException, InterruptedException {
            topK = context.getConfiguration().getInt("topK");
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            while (queue.size() != 0) {
                Movie movie = (Movie)queue.remove();
                context.write(new Text(movie.getTitle()), new Text(movie.getRating()));
            }
        }
	}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        int topK = Integer.parseInt(otherArgs[2]);
        conf.setInt("topK", topK);

		Job job = new Job(conf, "imdbstudent20191011");
		job.setJarByClass(IMDBStudent20191011.class);
		job.setMapperClass(IMDBStudent20191011Mapper.class);
		job.setCombinerClass(IMDBStudent20191011Reducer.class);
		job.setReducerClass(IMDBStudent20191011Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		FileSystem.get(job.getConfiguration()).delete( new Path(args[1]), true);
		job.waitForCompletion(true);
	}
}
