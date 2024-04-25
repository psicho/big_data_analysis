package com.skywaet.hadoop.h4;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CountableText implements Writable {

    private String value;
    private int count;

    public CountableText(String value, int count) {
        this.value = value;
        this.count = count;
    }

    public CountableText() {
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(value);
        dataOutput.writeInt(count);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        value = dataInput.readUTF();
        count = dataInput.readInt();
    }

    @Override
    public String toString() {
        return "value=" + value + "; amount=" + count;
    }

}
