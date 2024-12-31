package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1_Old {

public  class UniMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    private LongWritable matchCount = new LongWritable();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] parts = line.split("\t");
        if (parts.length >= 3) {
            String w = parts[0];
            long count = Long.parseLong(parts[2]);
            matchCount.set(count);
            CKey cKey = new CKey(w, "*");
            NKey nKey = new NKey(w, "*", "*");
            context.write(cKey, matchCount);
            context.write(nKey, matchCount);
        }
    }
}

    public class BiMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable matchCount = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length >= 3) {
                String[] words = parts[0].split(" ");
                if (words.length == 2) {
                    String w1 = words[0];
                    String w2 = words[1];
                    long count = Long.parseLong(parts[2]);
                    matchCount.set(count);
                    CKey cKey = new CKey(w1,w2);
                    NKey nKey = new NKey(w1, w2, "*");
                    context.write(cKey, matchCount);
                    context.write(nKey, matchCount);
                }
            }
        }
    }

    public class TriMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        private LongWritable matchCount = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] parts = line.split("\t");
            if (parts.length >= 3) {
                String[] words = parts[0].split(" ");
                if (words.length == 3) {
                    String w1 = words[0];
                    String w2 = words[1];
                    String w3 = words[2];
                    long count = Long.parseLong(parts[2]);
                    matchCount.set(count);
                    NKey nKey = new NKey(w1,w2,w3);
                    context.write(nKey, matchCount);
                }
            }
        }
    }
    // Key for N-type records
    public class NKey implements WritableComparable<NKey> {
        private String type = "N";    // Will always be "N"
        private String w3;
        private String w2;
        private String w1;

        public NKey(String w1, String w2, String w3) {
            this.w1 = w1;
            this.w2 = w2;
            this.w3 = w3;
        }

        @Override
        public int compareTo(NKey other) {
            int cmp = w3.compareTo(other.w3);
            if (cmp != 0) return cmp;

            cmp = w2.compareTo(other.w2);
            if (cmp != 0) return cmp;

            return w1.compareTo(other.w1);
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(w1);
            out.writeUTF(w2);
            out.writeUTF(w3);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = in.readUTF();
            w2 = in.readUTF();
            w3 = in.readUTF();
        }

        public String getW3(){
            return w3;
        }
    }

    // Key for C-type records
    public class CKey implements WritableComparable<CKey> {
        private String type = "C";    // Will always be "C"
        private String w2;
        private String w1;

        public CKey(String w1, String w2) {
            this.w1 = w1;
            this.w2 = w2;
        }
        @Override
        public int compareTo(CKey other) {
            int cmp = w2.compareTo(other.w2);
            if (cmp != 0) return cmp;

            return w1.compareTo(other.w1);
        }
        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(w1);
            out.writeUTF(w2);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = in.readUTF();
            w2 = in.readUTF();
        }

        public String getW2(){
            return w2;
        }
    }


    // Custom partitioner that handles both key types
    public class CustomPartitioner extends Partitioner<WritableComparable<?>, LongWritable> {// im not sure if its good, maybe we should add N,C to the hash
        @Override
        public int getPartition(WritableComparable<?> key, LongWritable value, int numReduceTasks) {
            if (key instanceof NKey) {
                NKey nKey = (NKey) key;
                // Partition N records by w3
                return (nKey.getW3().hashCode()) % numReduceTasks;
            } else {
                CKey cKey = (CKey) key;
                // Partition C records by w2
                return (cKey.getW2().hashCode()) % numReduceTasks;
            }
        }
    }

    public class CustomReducer extends Reducer<WritableComparable<?>, LongWritable, WritableComparable<?>, LongWritable> {
        @Override
        public void reduce(WritableComparable<?> key, Iterable<LongWritable> values, Context context) {
            if (key instanceof NKey) {
                // Handle N-type records
                // These will be grouped by w3 and sorted by w2,w1
            } else {
                // Handle C-type records
                // These will be grouped by w2 and sorted by w1
            }
        }
    }

//    @Override
//    public void setup(Context context)  throws IOException, InterruptedException {
//    }
//
//    @Override
//    public void map(K1 key, V1 value, Context context) throws IOException,  InterruptedException {
//    }
//
//    @Override
//    public void cleanup(Context context)  throws IOException, InterruptedException {
//    }
//
//  }
//
//  public static class ReducerClass extends Reducer<K2,V2,K3,V3> {
//
//	    @Override
//	    public void setup(Context context)  throws IOException, InterruptedException {
//	    }
//
//	    @Override
//	    public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
//	    }
//
//	    @Override
//	    public void cleanup(Context context)  throws IOException, InterruptedException {
//	    }
//	 }
//
//  public static class CombinerClass
//     extends Reducer<K2,V2,K3,V3> {
//
//	    @Override
//	    public void setup(Context context)  throws IOException, InterruptedException {
//	    }
//
//	    @Override
//	    public void reduce(K2 key, Iterable<V2> values, Context context) throws IOException,  InterruptedException {
//	    }
//
//	    @Override
//	    public void cleanup(Context context)  throws IOException, InterruptedException {
//	    }
//	 }
//
//  public static class PartitionerClass extends Partitioner<K2,V2> {
//
//      @Override
//      public int getPartition(K2 word, V2 count, int numReducers) {
//          return key.hashCode() % numReducers;
//      }
//  }


 public static void main(String[] args) throws Exception {

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "Word Prediction");
    job.setJarByClass(Step1_Old.class);
     // Set the appropriate mapper class based on the input data
     // Set the input paths and mappers for each n-gram
     MultipleInputs.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"), TextInputFormat.class, UniMapper.class);
     MultipleInputs.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"), TextInputFormat.class, BiMapper.class);
     MultipleInputs.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"), TextInputFormat.class, TriMapper.class);

    //job.setMapperClass(MapperClass.class);
    job.setPartitionerClass(CustomPartitioner.class);
    //job.setCombinerClass(CombinerClass.class);
    job.setReducerClass(CustomReducer.class);

    //job.setMapOutputKeyClass(K2.class);
    //job.setMapOutputValueClass(K2.class);
    //job.setOutputKeyClass(K3.class);
    //job.setOutputValueClass(K3.class);
     job.setMapOutputKeyClass(WritableComparable.class);
     job.setMapOutputValueClass(LongWritable.class);
     job.setOutputKeyClass(WritableComparable.class);
     job.setOutputValueClass(LongWritable.class);


     FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

//        For n_grams S3 files.
//        Note: This is English version and you should change the path to the relevant one
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setInputFormatClass(SequenceFileInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"));
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"));
        FileInputFormat.addInputPath(job, new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"));

    System.exit(job.waitForCompletion(true) ? 0 : 1);

  }
