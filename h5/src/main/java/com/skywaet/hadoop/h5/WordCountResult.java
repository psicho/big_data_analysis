package com.skywaet.hadoop.h5;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class WordCountResult implements WritableComparable<WordCountResult> {

    private String word;
    private String followedBy;
    private int count;

    public WordCountResult() {
    }

    public WordCountResult(String word, String followedBy, int count) {
        this.word = word;
        this.followedBy = followedBy;
        this.count = count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        String line = word + " " + followedBy + " " + count;
        dataOutput.writeUTF(line);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        String rawLine = dataInput.readUTF();
        String[] line = rawLine.split("\\s+");
        word = line[0];
        followedBy = line[1];
        count = Integer.parseInt(line[2], 10);
    }

    @Override
    public String toString() {
        return "word=" + word +
                "; followedBy=" + followedBy +
                "; amount=" + count;
    }

    public String getWord() {
        return word;
    }

    public String getFollowedBy() {
        return followedBy;
    }

    public int getCount() {
        return count;
    }

    @Override
    public int compareTo(WordCountResult o) {
        if (o == null) {
            return 1;
        }
        if (this.count != o.count) {
            return o.count - this.count;
        }
        if (!this.word.equals(o.word)) {
            return this.word.compareTo(o.word);
        }
        if (!this.followedBy.equals(o.followedBy)) {
            return this.followedBy.compareTo(o.followedBy);
        }
        return 0;
    }
}
