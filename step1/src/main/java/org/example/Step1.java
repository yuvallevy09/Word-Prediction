package org.example;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.apache.log4j.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Step1 {
    private static final Logger logger = Logger.getLogger(Step1.class);
    static Set<String> stopWords = new HashSet<>(Arrays.asList(
            "׳", "של", "רב", "פי", "עם", "עליו", "עליהם", "על", "עד", "מן", "מכל", "מי", "מהם", "מה", "מ",
            "למה", "לכל", "לי", "לו", "להיות", "לה", "לא", "כן", "כמה", "כלי", "כל", "כי", "יש", "ימים", "יותר",
            "יד", "י", "זה", "ז", "ועל", "ומי", "ולא", "וכן", "וכל", "והיא", "והוא", "ואם", "ו", "הרבה", "הנה",
            "היו", "היה", "היא", "הזה", "הוא", "דבר", "ד", "ג", "בני", "בכל", "בו", "בה", "בא", "את", "אשר", "אם",
            "אלה", "אל", "אך", "איש", "אין", "אחת", "אחר", "אחד", "אז", "אותו", "־", "^", "?", ";", ":", "1", ".",
            "-", "*", "\"", "!", "שלשה", "בעל", "פני", ")", "גדול", "שם", "עלי", "עולם", "מקום", "לעולם", "לנו",
            "להם", "ישראל", "יודע", "זאת", "השמים", "הזאת", "הדברים", "הדבר", "הבית", "האמת", "דברי", "במקום",
            "בהם", "אמרו", "אינם", "אחרי", "אותם", "אדם", "(", "חלק", "שני", "שכל", "שאר", "ש", "ר", "פעמים",
            "נעשה", "ן", "ממנו", "מלא", "מזה", "ם", "לפי", "ל", "כמו", "כבר", "כ", "זו", "ומה", "ולכל", "ובין",
            "ואין", "הן", "היתה", "הא", "ה", "בל", "בין", "בזה", "ב", "אף", "אי", "אותה", "או", "אבל", "א"
    ));

    public static class CompositeKey implements WritableComparable<CompositeKey> {
        private String type;     // "N" or "C"
        private String w1;
        private String w2;
        private String w3;

        public CompositeKey() {}

        public CompositeKey(String type, String w1, String w2, String w3) {
            this.type = type;
            this.w1 = w1;
            this.w2 = w2;
            this.w3 = w3;
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeUTF(type);
            out.writeUTF(w1);
            out.writeUTF(w2);
            out.writeUTF(w3);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            type = in.readUTF();
            w1 = in.readUTF();
            w2 = in.readUTF();
            w3 = in.readUTF();
        }

        @Override
        public int compareTo(CompositeKey other) {
            // First sort by type
            int cmp = type.compareTo(other.type);
            if (cmp != 0) return cmp;

            if (type.equals("N")) {
                // First compare w3 (primary grouping)
                cmp = w3.compareTo(other.w3);
                if (cmp != 0) return cmp;

                // Compare w2 only if both keys have a w2
                if (!w2.equals("*") && !other.w2.equals("*")) {
                    cmp = w2.compareTo(other.w2);
                    if (cmp != 0) return cmp; // if w2 = other.w2, cmp = 0 => continue to next if
                } else {
                    // If one has w2 and other doesn't, the one without w2 comes first
                    if (w2.equals("*")) return -1;
                    return 1; // Only other.w2 is "*"
                }

                // For records with same w2, sort by w1
                if (!w1.equals("*") && !other.w1.equals("*")) {
                    return w1.compareTo(other.w1);
                } else {
                    // If one has w1 and other doesn't, the one without w1 comes first
                    if (w1.equals("*")) return -1;
                    return 1;
                }
            } else { // C-type sorting logic
                // First by w2
                cmp = w2.compareTo(other.w2);
                if (cmp != 0) return cmp;

                // Then by w1 (for bigrams with same w2)
                // The one with w1="*" should come first
                if (w1.equals("*")) return -1;
                if (other.w1.equals("*")) return 1;
                return w1.compareTo(other.w1);
            }
        }

        public String getType() { return type; }
        public String getW1() { return w1; }
        public String getW2() { return w2; }
        public String getW3() { return w3; }
    }

    public static class NGramPartitioner extends Partitioner<CompositeKey, LongWritable> {
        @Override
        public int getPartition(CompositeKey key, LongWritable value, int numReduceTasks) {
            // For N-type, partition by w3
            // For C-type, partition by w2
            String partitionKey = key.getType() +
                    (key.getType().equals("N") ? key.getW3() : key.getW2());
            return Math.abs(partitionKey.hashCode() % numReduceTasks);
        }
    }

    public static class UniMapper extends Mapper<LongWritable, Text, CompositeKey, LongWritable> {
        public static enum Counters {
            TOTAL_WORDS
        }
        private final LongWritable count = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            logger.info("Processing UniGram: " + value.toString());
            logger.info("length: " + parts.length);
            if (parts.length >= 3) {
                String word = parts[0].trim();
                // Skip stop words
                if (!stopWords.contains(word)) {
                    long matchCount = Long.parseLong(parts[2]);
                    // Increment C0 counter
                    context.getCounter(Counters.TOTAL_WORDS).increment(matchCount);

                    count.set(matchCount);
                    // Emit for N1
                    context.write(new CompositeKey("N", "*", "*", word), count);
                    // Emit for C1
                    context.write(new CompositeKey("C", "*", word, "*"), count);
                    logger.info("Mapped UniGram: " + word + ", Count: " + matchCount);
                }
            }
            else {
                logger.warn("Invalid UniGram record: " + value.toString());
            }
        }
    }

    public static class BiMapper extends Mapper<LongWritable, Text, CompositeKey, LongWritable> {
        private final LongWritable count = new LongWritable();


        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            logger.info("Processing BiGram: " + value.toString());
            logger.info("length: " + parts.length);
            if (parts.length >= 3) {
                String[] words = parts[0].split(" ");
                if (words.length == 2) {
                    String w1 = words[0].trim();
                    String w2 = words[1].trim();

                    // Skip if either word is a stop word
                    if (stopWords.contains(w1) || stopWords.contains(w2)) return;

                    long matchCount = Long.parseLong(parts[2]);
                    count.set(matchCount);

                    // Only emit if this bigram will be needed
                    context.write(new CompositeKey("N", "*", w1, w2), count);
                    context.write(new CompositeKey("C", w1, w2, "*"), count); // w3 is not compared for C-Keys
                    logger.info("Mapped BiGram: " + w1 + " " + w2 + ", Count: " + matchCount);
                }
            }
            else {
                logger.warn("Invalid BiGram record: " + value.toString());
            }
        }
    }

    public static class TriMapper extends Mapper<LongWritable, Text, CompositeKey, LongWritable> {
        private final LongWritable count = new LongWritable();

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            logger.info("Processing TriGram: " + value.toString());
            if (parts.length >= 3) {
                String[] words = parts[0].split(" ");
                if (words.length == 3) {
                    String w1 = words[0].trim();
                    String w2 = words[1].trim();
                    String w3 = words[2].trim();

                    // Skip if any word is a stop word
                    if (stopWords.contains(w1) || stopWords.contains(w2) || stopWords.contains(w3)) return;

                    long matchCount = Long.parseLong(parts[2]);
                    count.set(matchCount);

                    // Emit for N3
                    context.write(new CompositeKey("N", w1, w2, w3), count);
                    logger.info("Mapped TriGram: " + w1 + " " + w2 + " "+ w3+", Count: " + matchCount);
                }
            }
            else {
                logger.warn("Invalid TriGram record: " + value.toString());
            }
        }
    }

    public static class NGramReducer extends Reducer<CompositeKey, LongWritable, Text, Text> {
        private long denW = 0;      // N1 or C1
        private long denPair = 0;   // N2
        private String currentType = null;
        private String currentPrimaryWord = null;
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();

        @Override
        public void reduce(CompositeKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            // Calculate sum of counts
            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            // Check if we're starting a new group
            if (currentType == null || !currentType.equals(key.getType()) ||
                    currentPrimaryWord == null ||
                    (key.getType().equals("N") && !currentPrimaryWord.equals(key.getW3())) ||
                    (key.getType().equals("C") && !currentPrimaryWord.equals(key.getW2()))) {

                currentType = key.getType();
                currentPrimaryWord = key.getType().equals("N") ? key.getW3() : key.getW2();
                denW = 0;
                denPair = 0;
            }

            if (key.getType().equals("N")) {
                // Handle N-type records
                if (key.getW2().equals("*") && key.getW1().equals("*")) {
                    denW = sum;  // N1
                } else if (key.getW1().equals("*")) {
                    denPair = sum;  // N2
                } else {
                    // N3 - Emit complete record
                    outputKey.set(String.format("N\t%s\t%s\t%s", key.getW1(), key.getW2(), key.getW3()));
                    outputValue.set(String.format("%d\t%d\t%d", denW, denPair, sum));
                    context.write(outputKey, outputValue);
                    logger.info("Reduced NGram: " + outputKey.toString() + ", Values: " + outputValue.toString());

                }
            } else {
                // Handle C-type records
                if (key.getW1().equals("*")) {
                    denW = sum;  // C1
                } else {
                    // C2 - Emit with C1
                    outputKey.set(String.format("C\t%s\t%s", key.getW1(), key.getW2()));
                    outputValue.set(String.format("%d\t%d", denW, sum));
                    context.write(outputKey, outputValue);
                    logger.info("Reduced CGram: " + outputKey.toString() + ", Values: " + outputValue.toString());

                }
            }
        }
    }

    public static class NGramCombiner extends Reducer<CompositeKey, LongWritable, CompositeKey, LongWritable> {
        private final LongWritable result = new LongWritable();

        @Override
        public void reduce(CompositeKey key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {
            // Simple sum of values
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
            logger.info("Combined: " + key.toString() + ", Sum: " + sum);
        }
    }

    public static void main(String[] args) throws Exception {
        // Input validation
//        if (args.length != 2) {
//            System.err.println("Usage: Step1 <input path> <output path>");
//            System.exit(2);
//        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Word Prediction Step 1");
        job.setJarByClass(Step1.class);

        // Set partitioner and combiner
        job.setPartitionerClass(NGramPartitioner.class);
        job.setCombinerClass(NGramCombiner.class);

        // Set reducer
        job.setReducerClass(NGramReducer.class);

        // Set output types
        job.setMapOutputKeyClass(CompositeKey.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Set input paths with appropriate mappers
//        MultipleInputs.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/1gram/data"),
//                SequenceFileInputFormat.class, UniMapper.class);
//        MultipleInputs.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/2gram/data"),
//                SequenceFileInputFormat.class, BiMapper.class);
//        MultipleInputs.addInputPath(job,
//                new Path("s3://datasets.elasticmapreduce/ngrams/books/20090715/heb-all/3gram/data"),
//                SequenceFileInputFormat.class, TriMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("s3://yuvalhagarwordprediction/1gram-test.txt"),
                TextInputFormat.class, UniMapper.class);;
        MultipleInputs.addInputPath(job,
                new Path("s3://yuvalhagarwordprediction/2gram-test.txt"),
                TextInputFormat.class, BiMapper.class);
        MultipleInputs.addInputPath(job,
                new Path("s3://yuvalhagarwordprediction/3gram-test.txt"),
                TextInputFormat.class, TriMapper.class);

        // Set output path
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path("s3://yuvalhagarwordprediction/output_step1"));

        // Set output format
        job.setOutputFormatClass(TextOutputFormat.class);

        // Run job and get C0
        boolean success = job.waitForCompletion(true);

        if (success) {
            // Get C0 value from counter
            long c0 = job.getCounters().findCounter(UniMapper.Counters.TOTAL_WORDS).getValue();
            
            // Write C0 to a file in the output directory
            Path outputPath = new Path("s3://yuvalhagarwordprediction/output_step1");
            Path c0File = new Path(outputPath, "C0.txt");
            
            FileSystem fs = c0File.getFileSystem(conf);
            try (FSDataOutputStream out = fs.create(c0File)) {
                out.writeBytes(String.valueOf(c0));
            }
        }
//        if (success) {
//            // Get C0 value from counter
//            long c0 = job.getCounters().findCounter(UniMapper.Counters.TOTAL_WORDS).getValue();
//            // Write C0 to a file in the output directory
//            Configuration conf2 = new Configuration();
//            conf2.set("fs.defaultFS", "s3://yuvalhagarwordprediction");
//            conf2.set("fs.s3a.endpoint", "s3.amazonaws.com");
//            org.apache.hadoop.fs.FileSystem fs = org.apache.hadoop.fs.FileSystem.get(conf2); //need to check
//            Path c0File = new Path("s3a://yuvalhagarwordprediction/output_step1/C0.txt");
//            try (org.apache.hadoop.fs.FSDataOutputStream out = fs.create(c0File)) {
//                out.writeBytes(String.valueOf(c0));
//                out.close();
//            }
//        }
        System.exit(success ? 0 : 1);
    }
}