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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step2 {
    public static class TrigramKey implements WritableComparable<TrigramKey> {
        private String w1;
        private String w2;
        private String w3;

        public TrigramKey() {
        }

        public TrigramKey(String w1, String w2, String w3) {
            this.w1 = w1;
            this.w2 = w2;
            this.w3 = w3;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, w1);
            Text.writeString(out, w2);
            Text.writeString(out, w3);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = Text.readString(in);
            w2 = Text.readString(in);
            w3 = Text.readString(in);
        }

        @Override
        public int compareTo(TrigramKey other) {
            // First compare w1
            int cmp = w1.compareTo(other.w1);
            if (cmp != 0) return cmp;

            // Then compare w2
            cmp = w2.compareTo(other.w2);
            if (cmp != 0) return cmp;

            // If one has w3="*", it comes first
            if (w3.equals("*")) return -1;
            if (other.w3.equals("*")) return 1;

            // Otherwise, compare w3
            return w3.compareTo(other.w3);
        }
    }

    public static class TrigramPartitioner extends Partitioner<TrigramKey, Text> {
        @Override
        public int getPartition(TrigramKey key, Text value, int numReduceTasks) {
            return (key.w1 + " " + key.w2).hashCode() % numReduceTasks;
        }
    }

    public static class Step2Mapper extends Mapper<LongWritable, Text, TrigramKey, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                String[] parts = value.toString().split("\t");
                String type = parts[0];

                if (type.equals("C")) {
                    // Input format: C w1 w2 C1 C2
                    context.write(new TrigramKey(parts[1], parts[2], "*"),
                            new Text(parts[3] + "\t" + parts[4]));
                } else if (type.equals("N")) {
                    // Input format: N w1 w2 w3 N1 N2 N3
                    context.write(new TrigramKey(parts[1], parts[2], parts[3]),
                            new Text(parts[4] + "\t" + parts[5] + "\t" + parts[6]));
                }
            } catch (Exception e) {
                context.getCounter("Error", "MapperError").increment(1);
            }
        }
    }

    public static class ProbabilityReducer extends Reducer<TrigramKey, Text, Text, Text> {
        private String currentW1 = null;
        private String currentW2 = null;
        private long[] cValues = null;  // [C0, C1, C2]
        private long C0;
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
//            try {
//                Configuration conf = context.getConfiguration();
//                Path c0File = new Path(conf.get("c0_path"));
//                org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
//                try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(c0File)) {
//                    C0 = Long.parseLong(in.readLine().trim());
//                    if (C0 <= 0) {
//                        throw new IOException("Invalid C0 value: " + C0);
//                    }
//                }
//            } catch (Exception e) {
//                context.getCounter("Error", "C0ReadError").increment(1);
//                throw new IOException("Failed to read C0", e);
//            }
            C0 = 100;

        }

        @Override
        public void reduce(TrigramKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            try {
                if (currentW1 == null || !currentW1.equals(key.w1) ||
                        !currentW2.equals(key.w2) || key.w3.equals("*")) {

                    currentW1 = key.w1;
                    currentW2 = key.w2;

                    if (key.w3.equals("*")) {
                        // Process C record
                        String[] parts = values.iterator().next().toString().split("\t");
                        cValues = new long[]{C0,
                                Long.parseLong(parts[0]),  // C1
                                Long.parseLong(parts[1])}; // C2

                        if (cValues[1] <= 0 || cValues[2] <= 0) {
                            context.getCounter("Error", "InvalidCValues").increment(1);
                            cValues = null;
                        }
                        return;
                    }
                }

                // Skip if no valid C values
                if (cValues == null) return;

                // Process N records
                for (Text value : values) {
                    String[] parts = value.toString().split("\t");
                    long N1 = Long.parseLong(parts[0]);
                    long N2 = Long.parseLong(parts[1]);
                    long N3 = Long.parseLong(parts[2]);

                    // Calculate k2 and k3
                    double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
                    double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

                    // Calculate probability
                    double prob = k3 * (N3 / (double)cValues[2]) +
                            (1 - k3) * k2 * (N2 / (double)cValues[1]) +
                            (1 - k3) * (1 - k2) * (N1 / (double)cValues[0]);

                    if (!Double.isNaN(prob) && !Double.isInfinite(prob) && prob >= 0 && prob <= 1) {
                        outputKey.set(String.format("%s %s %s", currentW1, currentW2, key.w3));
                        outputValue.set(String.format("%.3f", prob));
                        context.write(outputKey, outputValue);
                    }
                }
            } catch (Exception e) {
                context.getCounter("Error", "ReduceError").increment(1);
            }
        }
    }

    public static void main(String[] args) throws Exception {
//        if (args.length != 3) {
//            System.err.println("Usage: Step2 <input path> <output path> <c0 file path>");
//            System.exit(2);
//        }

        Configuration conf = new Configuration();
//        conf.set("c0_path", "s3://yuvalhagarwordprediction/output_step1" + "/C0.txt");

        Job job = Job.getInstance(conf,"Word Prediction Step 2");
        job.setJarByClass(Step2.class);

        job.setMapperClass(Step2Mapper.class);
        job.setPartitionerClass(TrigramPartitioner.class);
        job.setReducerClass(ProbabilityReducer.class);

        job.setMapOutputKeyClass(TrigramKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path("s3://yuvalhagarwordprediction/output_step1"));
        FileOutputFormat.setOutputPath(job, new Path("s3://yuvalhagarwordprediction/output_step2"));

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}