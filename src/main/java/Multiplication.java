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
			String[] movieBRelation = value.toString().trim().split("\t");
			context.write(new Text(movieBRelation[0]), new Text(movieBRelation[1]));
		}
	}

	public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {

		// map method
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			//input: user,movie,rating
			//pass data to reducer
            String[] userMovieRation = value.toString().trim().split(",");
            context.write(new Text(userMovieRation[1]), new Text(userMovieRation[0] + ":" + userMovieRation[2]));
		}
	}

	public static class MultiplicationReducer extends Reducer<Text, Text, Text, DoubleWritable> {
		// reduce method
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

			//key = movieB value = <movieA=relation, movieC=relation... userA:rating, userB:rating...>
			//collect the data for each movie, then do the multiplication
            Map<String, Double> coOccurrenceMap = new HashMap<>();
            Map<String, Double> ratingMap = new HashMap<>();
            for (Text value : values) {
                if (value.toString().contains("=")) {
                    String[] movieRelation = value.toString().trim().split("=");
                    coOccurrenceMap.put(movieRelation[0], Double.parseDouble(movieRelation[1]));
                } else {
                    String[] userRating = values.toString().trim().split("=");
                    ratingMap.put(userRating[0], Double.parseDouble(userRating[1]));
                }
            }

            for (Map.Entry<String, Double> entry : coOccurrenceMap.entrySet()) {
                String movieId = entry.getKey();
                double relation = entry.getValue();

                for (Map.Entry<String, Double> element : ratingMap.entrySet()) {
                    String userId = element.getKey();
                    double rating = element.getValue();

                    String outputKey = userId + ":" + movieId;
                    double outputValue = rating * relation;

                    context.write(new Text(outputKey), new DoubleWritable(outputValue));
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
