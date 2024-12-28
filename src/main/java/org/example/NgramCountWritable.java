package org.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class NgramCountWritable extends IntWritable implements Writable {
    private String ngram;
    private long count;

    public NgramCountWritable() {}

    public NgramCountWritable(String ngram, long count) {
        this.ngram = ngram;
        this.count = count;
    }

    public String getNgram() {
        return ngram;
    }

    public void setNgram(String ngram) {
        this.ngram = ngram;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public int getN() {
        return ngram.split(" ").length;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(ngram);
        out.writeLong(count);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        ngram = in.readUTF();
        count = in.readLong();
    }
}{
}
