package ir.ac.ut.iis.ppr;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class StringWritable implements WritableComparable {

    private String value;

    public StringWritable() {

    }

    public StringWritable(String value) {
        set(value);
    }

    public void set(String value) {
        this.value = new String(value);
    }

    public String get() { return value; }

    public boolean equals(Object o) {
        if (!(o instanceof StringWritable)) {
            return false;
        }
        StringWritable other = (StringWritable)o;
        return this.value.equals(other.value);
    }

    public void readFields(DataInput in) throws IOException {
        value = in.readUTF();
    }

    public void write(DataOutput out) throws IOException {
        out.writeUTF(value);
    }

    public int compareTo(Object o) {
        StringWritable other = (StringWritable)o;
        return value.compareTo(other.value);
    }
}
