package com.skywaet.hadoop.h6;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RawSpectrum implements Writable {
  public static final Pattern FILENAME_PATTERN =
      Pattern.compile("([0-9]{5})([dijkw])([0-9]{4})\\.txt\\.gz");

  public RawSpectrum() {}

  public void setField(String filename, float[] val) {
    Matcher matcher = FILENAME_PATTERN.matcher(filename);
    if (matcher.matches()) {
      String field = matcher.group(2);
      if (field.equals("i")) i = val;
      if (field.equals("j")) j = val;
      if (field.equals("k")) k = val;
      if (field.equals("w")) w = val;
      if (field.equals("d")) d = val;
    }
  }

  public void setField2(String field, float[] val) {
    if (field.equals("i")) i = val;
    if (field.equals("j")) j = val;
    if (field.equals("k")) k = val;
    if (field.equals("w")) w = val;
    if (field.equals("d")) d = val;
  }

  public int size() {
    int n = 0;
    if (i != null) ++n;
    if (j != null) ++n;
    if (k != null) ++n;
    if (w != null) ++n;
    if (d != null) ++n;
    return n;
  }

  public boolean isValid() {
    return size() == 5;
  }

  public double variance() {
    int n = Math.min(i.length, j.length);
    n = Math.min(n, k.length);
    n = Math.min(n, w.length);
    n = Math.min(n, d.length);
    double theta0 = 0, theta1 = 2.0*Math.PI;
    double dtheta = (theta1-theta0)/n;
    double sum = 0;
    for (int i=0; i<n; ++i) {
      for (int j=0; j<n; ++j) {
        double theta = theta0 + dtheta*j;
        double density = this.w[i];
        double r1 = this.j[i];
        double r2 = this.k[i];
        double alpha1 = this.d[i];
        double alpha2 = this.i[i];
        sum += density * (1.0/Math.PI) *
          (0.5 + 0.01*r1*Math.cos(theta - alpha1) + 0.01*r2*Math.cos(2.0*(theta - alpha2)));
      }
    }
    return sum;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    writeFloatArray(out, i);
    writeFloatArray(out, j);
    writeFloatArray(out, k);
    writeFloatArray(out, w);
    writeFloatArray(out, d);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    i = readFloatArray(in);
    j = readFloatArray(in);
    k = readFloatArray(in);
    w = readFloatArray(in);
    d = readFloatArray(in);
  }

  private void writeFloatArray(DataOutput out, float[] xs) throws IOException {
    out.writeInt(xs.length);
    for (float x : xs) {
      out.writeFloat(x);
    }
  }

  private float[] readFloatArray(DataInput in) throws IOException {
    int size = in.readInt();
    float[] xs = new float[size];
    for (int i = 0; i < xs.length; ++i) {
      xs[i] = in.readFloat();
    }
    return xs;
  }

  @Override
  public String toString() {
    return "[i="
        + Arrays.toString(i)
        + ",j="
        + Arrays.toString(j)
        + ",k="
        + Arrays.toString(k)
        + ",w="
        + Arrays.toString(w)
        + ",d="
        + Arrays.toString(d)
        + "]";
  }

  private float[] i = null, j = null, k = null, w = null, d = null;
}
