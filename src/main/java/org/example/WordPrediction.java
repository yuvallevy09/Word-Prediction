package org.example;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import java.util.StringTokenizer;

public class WordPrediction {

public static class MapperClass extends Mapper<LongWritable, Text, Text, IntWritable> {
       // private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException,  InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length >= 4) {
                String trigram = parts[0], year = parts[1], matchCount = parts[2], volumeCount = parts[3];
                StringTokenizer itr = new StringTokenizer(trigram);
                while (itr.hasMoreTokens()) {
                    word.set(itr.nextToken());
                    context.write(word, new IntWritable(Integer.parseInt(matchCount)));
                }
            }
        }

    @Override
    public void setup(Context context)  throws IOException, InterruptedException {
    }

    @Override
    public void map(K1 key, V1 value, Context context) throws IOException,  InterruptedException {
    }

    @Override
    public void cleanup(Context context)  throws IOException, InterruptedException {
    }

  }

  public static class ReducerClass extends Reducer<K2,V2,K3,V3> {

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
	    }

	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }

  public static class CombinerClass
     extends Reducer<K2,V2,K3,V3> {

	    @Override
	    public void setup(Context context)  throws IOException, InterruptedException {
	    }

	    @Override
	    public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
	    }

	    @Override
	    public void cleanup(Context context)  throws IOException, InterruptedException {
	    }
	 }

  public static class PartitionerClass extends Partitioner<K2,V2> {

      @Override
      public int getPartition(K2 word, V2 count, int numReducers) {
        return key.hashCode() % numReducers;
      }

    }


 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Word Prediction");
    job.setJarByClass(WordPrediction.class);

    job.setMapperClass(MapperClass.class);
    //job.setPartitionerClass(PartitionerClass.class);
    //job.setCombinerClass(CombinerClass.class);
    job.setReducerClass(ReducerClass.class);

    //job.setMapOutputKeyClass(K2.class);
    //job.setMapOutputValueClass(K2.class);
    //job.setOutputKeyClass(K3.class);
    //job.setOutputValueClass(K3.class);

    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }

}