package task1;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by filipgulan on 07/04/2017.
 */
public class TripTimeReducer extends Reducer<Text, TripTimeTuple, Text, TripTimeTuple> {

    private TripTimeTuple result = new TripTimeTuple();

    @Override
    protected void reduce(Text key, Iterable<TripTimeTuple> values, Context context)
            throws IOException, InterruptedException {
        Integer totalDuration = 0, minDuration = -1, maxDuration = -1;
        Integer currentMin, currentMax;

        for (TripTimeTuple value : values) {
            totalDuration += value.getTotalDuration().get();

            if (minDuration == -1) {
                minDuration = value.getMinDuration().get();
                maxDuration = value.getMaxDuration().get();
            } else {
                currentMin = value.getMinDuration().get();
                currentMax = value.getMaxDuration().get();
                minDuration = currentMin < minDuration ? currentMin : minDuration;
                maxDuration = currentMax > maxDuration ? currentMax : maxDuration;
            }
        }
        result.setData(totalDuration, minDuration, maxDuration);
        context.write(key, result);
    }
}
