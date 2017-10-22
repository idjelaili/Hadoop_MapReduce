package miage.bigdata.question;

import static java.lang.Double.parseDouble;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.common.collect.MinMaxPriorityQueue;

import miage.bigdata.utils.Country;

public class Question3_0 {

	/**
	 *
	 * @author nadjik
	 *
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		@Override
		protected void map(final LongWritable key, final Text value, final Context context)
				throws IOException, InterruptedException {

			// HashMap<String, String> hmap = new HashMap<String, String>();
			String[] tagTable = value.toString().split("\t");
			String tag = java.net.URLDecoder.decode(tagTable[8], "UTF-8");
			String altitude = tagTable[10];
			String accuracy = tagTable[11];
			String tagCodePays;
			Country codePays = Country.getCountryAt(parseDouble(altitude), parseDouble(accuracy));
			if (codePays == null) {
				System.out.println("Le code n'existe pas");
			} else {
				for (String tagSend : tag.toString().split(",")) {
					tagCodePays = codePays.toString().concat(",").concat(tagSend);
					context.write(new Text(tagCodePays), new IntWritable(1));
				}
			}
		}
	}

	/**
	 *
	 * @author nadjik
	 *
	 */
	public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
				throws IOException, InterruptedException {

			// HashMap<String, Integer> hmap = new HashMap<String, Integer>();
			// String paysSplit;
			// String tagSplit;
			Integer sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			// tagSplit = key.toString().split("-")[1];
			// hmap.put(tagSplit, sum);
			context.write(key, new IntWritable(sum));
		}
	}

	/**
	 *
	 * @author nadjik
	 *
	 */
	/*
	 * public static class MyMapper1 extends Mapper<Text, IntWritable, Text,
	 * StringAndInt> {
	 * 
	 * @Override protected void map(LongWritable key, Text value, Context
	 * context) throws IOException, InterruptedException {
	 * 
	 * // HashMap<String, String> hmap = new HashMap<String, String>(); String[]
	 * tagTable = value.toString().split("\t"); String tag =
	 * java.net.URLDecoder.decode(tagTable[8], "UTF-8"); String altitude =
	 * tagTable[10]; String accuracy = tagTable[11]; String tagCodePays; Country
	 * codePays = Country.getCountryAt(parseDouble(altitude),
	 * parseDouble(accuracy)); if (codePays == null) { System.out.println(
	 * "Le code n'existe pas"); } else { for (String tagSend :
	 * tag.toString().split(",")) { tagCodePays =
	 * codePays.toString().concat(",").concat(tagSend); context.write(new
	 * Text(tagCodePays), new IntWritable(1)); } } } }
	 */

	/**
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {
		
		//MinMaxPriorityQueue<StringAndInt> minMax = new MinMaxPriorityQueue<StringAndInt>(null, 0);

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question3_0");
		job.setJarByClass(Question3_0.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		// 1.4 Job Combiner // job.setCombinerClass(MyReducer.class);

		// 1.5 Nombre de reducers

		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
