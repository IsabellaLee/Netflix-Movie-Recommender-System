import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class Normalize {

    public static class NormalizeMapper extends Mapper<LongWritable, Text, Text, Text> {

        // map method
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            //movieA:movieB \t relation
            //collect the relationship list for movieA
            String[] movieA_movieB_relation = value.toString().trim().split("\t");
            String relation = movieA_movieB_relation[1];
            String[] movieA_movieB = movieA_movieB_relation[0].trim().split(":");
            String movieA = movieA_movieB[0];
            String movieB = movieA_movieB[1];
            context.write(new Text(movieA), new Text(movieB + "=" + relation));
        }
    }

    public static class NormalizeReducer extends Reducer<Text, Text, Text, Text> {
        // reduce method
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //key = movieA, value=<movieB:relation, movieC:relation...>
            //normalize each unit of co-occurrence matrix
            int sum = 0;
            Map<String, Integer> map = new HashMap<String, Integer>();
            for (Text value : values) {

                int relation = Integer.parseInt(value.toString().trim().split("=")[1]);
                sum += relation;
                map.put(value.toString().trim().split("=")[0], relation);

            }

            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                String outputkey = entry.getKey();
                String outputvalue = key.toString() + "=" + (double) entry.getValue()/sum;
                context.write(new Text(outputkey), new Text(outputvalue));
            }
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        job.setMapperClass(NormalizeMapper.class);
        job.setReducerClass(NormalizeReducer.class);

        job.setJarByClass(Normalize.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        TextInputFormat.setInputPaths(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
