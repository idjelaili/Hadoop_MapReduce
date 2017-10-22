package miage.bigdata.question;

import static java.lang.Double.parseDouble;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class Question2_1 {

	/**
	 *
	 * @author nadjik
	 *
	 */
	public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

		@Override
		protected void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {

			String[] tagTable = value.toString().split("\t");
			String tag = java.net.URLDecoder.decode(tagTable[8], "UTF-8");
			String altitude = tagTable[10];
			String accuracy = tagTable[11];
			Country codePays = Country.getCountryAt(parseDouble(altitude), parseDouble(accuracy));
			if (codePays == null) {
				System.out.println("Le code n'existe pas");
			} else {
				for (String tagSend : tag.toString().split(",")) {
					context.write(new Text(codePays.toString()),new Text(tagSend) );
				}
			}
			
		}
	}

	/**
	 *
	 * @author nadjik
	 *
	 */
	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(final Text key, final Iterable<Text> values, final Context context)
				throws IOException, InterruptedException {
			Map<String, Integer> mapTags = new HashMap<>();
			MinMaxPriorityQueue<StringAndInt> minMaxPriorityQueue = MinMaxPriorityQueue.maximumSize(2).create();
			Iterator<Text> iterator = values.iterator();
			while (iterator.hasNext()) {
				String tag = iterator.next().toString();

				if (!mapTags.containsKey(tag)) {
					mapTags.put(tag, 1);
				} else {
					mapTags.put(tag, mapTags.get(tag) + 1);
				}
			}
			for (Entry<String, Integer> stringAndInt : mapTags.entrySet()) {
				StringAndInt stringAndIntObject = new StringAndInt(stringAndInt.getKey(), stringAndInt.getValue());
				minMaxPriorityQueue.add(stringAndIntObject);
			}

			Iterator<StringAndInt> iterator2 = minMaxPriorityQueue.iterator();
			while (iterator2.hasNext()) {
				StringAndInt tag2 = iterator2.next();
				context.write(new Text(key), new Text(tag2.getTag() + "-" + tag2.getNbOccurTag()));
			}

		}
	}

	/**
	 *
	 * @param args
	 * @throws Exception
	 */
	public static void main(final String[] args) throws Exception {

		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		String input = otherArgs[0];
		String output = otherArgs[1];

		Job job = Job.getInstance(conf, "Question2_1");
		job.setJarByClass(Question2_1.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// 1.4 Job Combiner
		// job.setCombinerClass(MyReducer.class);

		// 1.5 Nombre de reducers
		job.setNumReduceTasks(3);

		FileInputFormat.addInputPath(job, new Path(input));
		job.setInputFormatClass(TextInputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(output));
		job.setOutputFormatClass(TextOutputFormat.class);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}