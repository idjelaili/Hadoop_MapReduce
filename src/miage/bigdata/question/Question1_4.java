package miage.bigdata.question;

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

import miage.bigdata.question.Question1_4.MyReducer;

public class Question1_4 {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        public void map(final LongWritable cle, final Text valeur, final Context context)
                throws IOException, InterruptedException {

            String line = valeur.toString();

            // Suppression de l'espace
            String[] words = line.split(" ");

            // Parcour de l'ensemble des mots
            for (String word : words) {
                Text cleDeSorti = new Text(word.toUpperCase().trim());
                IntWritable valeurDeSorti = new IntWritable(1);
                context.write(cleDeSorti, valeurDeSorti);
            }
        }
    }

    public static class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(final Text mot, final Iterable<IntWritable> valeurs, final Context context)
                throws IOException, InterruptedException {

            int somme = 0;
            for (IntWritable value : valeurs) {
                somme += value.get();
            }
            context.write(mot, new IntWritable(somme));
        }
    }

    public static void main(final String[] args) throws Exception {

        Configuration configuration = new Configuration();
        String[] otherArgs = new GenericOptionsParser(configuration, args).getRemainingArgs();
        String input = otherArgs[0];
        String output = otherArgs[1];

        Job job = Job.getInstance(configuration, "question1_4");
        job.setJarByClass(Question1_4.class);

        job.setMapperClass(MyMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(MyReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(input));
        job.setInputFormatClass(TextInputFormat.class);

        FileOutputFormat.setOutputPath(job, new Path(output));
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setCombinerClass(MyReducer.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
