package task1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by filipgulan on 07/04/2017.
 */
public class TripTimeTuple implements WritableComparable<TripTimeTuple> {

    private IntWritable minDuration;
    private IntWritable maxDuration;
    private IntWritable totalDuration;

    public TripTimeTuple() {
        this(new IntWritable(), new IntWritable(),
                new IntWritable());
    }

    public TripTimeTuple(IntWritable minDuration, IntWritable maxDuration,
                         IntWritable totalDuration) {
        this.minDuration = minDuration;
        this.maxDuration = maxDuration;
        this.totalDuration = totalDuration;
    }

    public TripTimeTuple(Integer minDuration, Integer maxDuration,
                         Integer totalDuration) {
        setData(totalDuration, minDuration, maxDuration);
    }

    @Override
    public int compareTo(TripTimeTuple tuple) {
        Integer result = totalDuration.compareTo(tuple.totalDuration);
        if (result != 0 ) {
            return result;
        }
        result = maxDuration.compareTo(tuple.maxDuration);
        if (result != 0 ) {
            return result;
        }

        return minDuration.compareTo(tuple.minDuration);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        minDuration.write(dataOutput);
        maxDuration.write(dataOutput);
        totalDuration.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        minDuration.readFields(dataInput);
        maxDuration.readFields(dataInput);
        totalDuration.readFields(dataInput);
    }

    public IntWritable getMinDuration() {
        return minDuration;
    }

    public IntWritable getMaxDuration() {
        return maxDuration;
    }

    public IntWritable getTotalDuration() {
        return totalDuration;
    }

    public void setData(Integer totalDuration, Integer minDuration,
                        Integer maxDuration) {
        this.totalDuration = new IntWritable(totalDuration);
        this.minDuration = new IntWritable(minDuration);
        this.maxDuration = new IntWritable(maxDuration);
    }

    public int hashCode() {
        return totalDuration.hashCode() * 163
                + maxDuration.hashCode() + minDuration.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TripTimeTuple) {
            TripTimeTuple tp = (TripTimeTuple) o;
            return totalDuration.equals(tp.totalDuration)
                    && minDuration.equals(tp.minDuration)
                    && maxDuration.equals(tp.maxDuration);
        }
        return false;
    }

    @Override
    public String toString() {
        return totalDuration.toString() + "\t" + minDuration.toString()
                + "\t" + maxDuration.toString();
    }
}
