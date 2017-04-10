import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Multiplication {
	public static class CooccurrenceMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//input: movieB \t movieA=relation

			//pass data to reducer
			String[] movieB_movieA_relation = value.toString().trim().split("\t");
			context.write(new Text(movieB_movieA_relation[0]), new Text(movieB_movieA_relation[1]));

		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer
			String[] user_movie_rating = value.toString().trim().split(",");
			String user = user_movie_rating[0];
			String movie = user_movie_rating[1];
			String rating = user_movie_rating[2];

			context.write(new Text(movie), new Text(user + ":" + rating));

		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
			Map<String, Double> movie_relation = new HashMap<String, Double>();
			Map<String, Double> user_rating = new HashMap<String, Double>();

			for (Text value : values) {
				String line = value.toString().trim();
				if (line.contains("=")) {
					String movie = line.split("=")[0];
					String relation = line.split("=")[1];
					movie_relation.put(movie, Double.parseDouble(relation));
				}
				else
				{
					String user = line.split(":")[0];
					String rating = line.split(":")[1];
					user_rating.put(user, Double.parseDouble(rating));
				}
			}

			for (Map.Entry<String, Double> entry : user_rating.entrySet()) {
				for (Map.Entry<String, Double> entry1 : movie_relation.entrySet()) {
					String outputkey = entry.getKey() + ":" + entry1.getKey();
					double outputvalue = entry.getValue() * entry1.getValue();
					context.write(new Text(outputkey), new DoubleWritable(outputvalue));
				}
 			}

		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);
		job.setJarByClass(Multiplication.class);

		ChainMapper.addMapper(job, CooccurrenceMapper.class, LongWritable.class, Text.class, Text.class, Text.class, conf);
		ChainMapper.addMapper(job, RatingMapper.class, Text.class, Text.class, Text.class, Text.class, conf);

		job.setMapperClass(CooccurrenceMapper.class);
		job.setMapperClass(RatingMapper.class);

		job.setReducerClass(MultiplicationReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CooccurrenceMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, RatingMapper.class);

		TextOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.waitForCompletion(true);
	}
}
