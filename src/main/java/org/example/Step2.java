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
import java.util.*;

public class Step2 {

    // Simple key class just for grouping by w1,w2
    public static class BigramKey implements WritableComparable<BigramKey> {
        private String w1;
        private String w2;

        public BigramKey() {}

        public BigramKey(String w1, String w2) {
            this.w1 = w1;
            this.w2 = w2;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            Text.writeString(out, w1);
            Text.writeString(out, w2);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            w1 = Text.readString(in);
            w2 = Text.readString(in);
        }

        @Override
        public int compareTo(BigramKey other) {
            return 0; // Order doesn't matter, just need grouping
        }

        @Override
        public int hashCode() {
            return (w1 + " " + w2).hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof BigramKey) {
                BigramKey other = (BigramKey) obj;
                return w1.equals(other.w1) && w2.equals(other.w2);
            }
            return false;
        }
    }

    // Mapper for C-type records
    public static class CMapper extends Mapper<LongWritable, Text, BigramKey, Text> {
        private long C0;
        private final Text outputValue = new Text();
        private Map<BigramKey, String> bigramCValues = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path c0File = new Path(conf.get("c0_path"));
            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf);
            try (org.apache.hadoop.fs.FSDataInputStream in = fs.open(c0File)) {
                C0 = Long.parseLong(in.readLine().trim());
            }
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts[0].equals("C")) {
                String w1 = parts[1];
                String w2 = parts[2];
                BigramKey bigramKey = new BigramKey(w1, w2);
                // Store C0, C1, C2
                String valueStr = String.format("C\t%d\t%d\t%d", C0, Long.parseLong(parts[3]), Long.parseLong(parts[4]));
                bigramCValues.put(bigramKey, valueStr);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<BigramKey, String> entry : bigramCValues.entrySet()) {
                outputValue.set(entry.getValue());
                context.write(entry.getKey(), outputValue);
            }
        }
    }

    // Mapper for N-type records
    public static class NMapper extends Mapper<LongWritable, Text, BigramKey, Text> {
        private final Text outputValue = new Text();
        private Map<BigramKey, Set<String>> bigramNValues = new HashMap<>();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts[0].equals("N")) {
                String w1 = parts[1];
                String w2 = parts[2];
                String w3 = parts[3];
                BigramKey bigramKey = new BigramKey(w1, w2);

                // Store N1, N2, N3, w3
                String valueStr = String.format("N\t%d\t%d\t%d\t%s",
                        Long.parseLong(parts[4]),
                        Long.parseLong(parts[5]),
                        Long.parseLong(parts[6]),
                        w3);

                bigramNValues.computeIfAbsent(bigramKey, k -> new HashSet<>()).add(valueStr);
            }
        }

        public static class BigramPartitioner extends Partitioner<BigramKey, Text> {
            @Override
            public int getPartition(BigramKey key, Text value, int numReduceTasks) {
                // Use w1 and w2 to determine the partition
                return (key.w1 + " " + key.w2).hashCode() % numReduceTasks;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<BigramKey, Set<String>> entry : bigramNValues.entrySet()) {
                for (String value : entry.getValue()) {
                    outputValue.set(value);
                    context.write(entry.getKey(), outputValue);
                }
            }
        }
    }

    // Combiner to reduce data transfer
    public static class ProbabilityCombiner extends Reducer<BigramKey, Text, BigramKey, Text> {
        @Override
        public void reduce(BigramKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String cValue = null;
            Set<String> nValues = new HashSet<>();

            for (Text value : values) {
                String valueStr = value.toString();
                if (valueStr.startsWith("C")) {
                    cValue = valueStr;
                } else {
                    nValues.add(valueStr);
                }
            }

            if (cValue != null) {
                context.write(key, new Text(cValue));
            }
            for (String nValue : nValues) {
                context.write(key, new Text(nValue));
            }
        }
    }

    // Reducer to calculate probabilities
    public static class ProbabilityReducer extends Reducer<BigramKey, Text, Text, Text> {
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        public void reduce(BigramKey key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Store C values and N records
            long C0 = 0, C1 = 0, C2 = 0;
            List<String> nRecords = new ArrayList<>();

            // First pass to get all values
            for (Text value : values) {
                String[] parts = value.toString().split("\t");
                if (parts[0].equals("C")) {
                    C0 = Long.parseLong(parts[1]);
                    C1 = Long.parseLong(parts[2]);
                    C2 = Long.parseLong(parts[3]);
                } else {
                    nRecords.add(value.toString());
                }
            }

            // Calculate probability for each N record
            for (String record : nRecords) {
                String[] parts = record.split("\t");
                long N1 = Long.parseLong(parts[1]);
                long N2 = Long.parseLong(parts[2]);
                long N3 = Long.parseLong(parts[3]);
                String w3 = parts[4];

                // Calculate k2 and k3
                double k2 = (Math.log(N2 + 1) + 1) / (Math.log(N2 + 1) + 2);
                double k3 = (Math.log(N3 + 1) + 1) / (Math.log(N3 + 1) + 2);

                // Calculate final probability
                double prob = k3 * (N3 / (double)C2) +
                        (1 - k3) * k2 * (N2 / (double)C1) +
                        (1 - k3) * (1 - k2) * (N1 / (double)C0);

                // Emit result
                outputKey.set(String.format("%s %s %s", key.w1, key.w2, w3));
                outputValue.set(String.format("%.6f", prob));
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: Step2 <input path> <output path> <c0 file path>");
            System.exit(2);
        }

        Configuration conf = new Configuration();
        conf.set("c0_path", args[2]);

        Job job = Job.getInstance(conf, "Word Prediction Step 2");
        job.setJarByClass(Step2.class);

        // Set mappers and reducer
        job.setMapperClass(CMapper.class);
        job.setMapperClass(NMapper.class);
        job.setCombinerClass(ProbabilityCombiner.class);
        job.setPartitionerClass(NMapper.BigramPartitioner.class);
        job.setReducerClass(ProbabilityReducer.class);

        // Set output types
        job.setMapOutputKeyClass(BigramKey.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input/output paths
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set input/output formats
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}