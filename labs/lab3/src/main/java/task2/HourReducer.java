package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import util.Pair;
import util.RecordParser;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by filipgulan on 15/04/2017.
 */
public class HourReducer extends Reducer<IntWritable, Text, NullWritable, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Map<String, Double> amounts = new HashMap<>();
        Map<String, Integer> drives = new HashMap<>();
        RecordParser parser = new RecordParser();

        for (Text row : values) {
            try {
                parser.parse(row.toString());
                String cellKey = parser.getCellKey();

                putDrive(cellKey, drives);
                putAmount(cellKey, parser.getTotalAmount(), amounts);
            } catch (Exception e) {
                // Do nothing
            }
        }
        Pair<String, Double> maxAmount = new Pair<>("", 0.0);
        Pair<String, Integer> maxDrives = new Pair<>("", 0);

        Double amountSum = 0.0;
        for (Map.Entry<String, Double> entry : amounts.entrySet()) {
            maxAmount = entry.getValue() > maxAmount.getValue() ? new Pair<>(entry) : maxAmount;
            amountSum += entry.getValue();
        }

        Integer drivesSum = 0;
        for (Map.Entry<String, Integer> entry : drives.entrySet()) {
            maxDrives = entry.getValue() > maxDrives.getValue() ? new Pair<>(entry) : maxDrives;
            drivesSum += entry.getValue();
        }

        String output =
                "hour: " + key.toString() + System.lineSeparator() +
                        "drives: " + maxDrives.getKey() + " " + drivesSum + System.lineSeparator() +
                        "amount: " + maxAmount.getKey() + " " + amountSum;
        context.write(NullWritable.get(), new Text(output));
    }

    private void putDrive(String cellKey, Map<String, Integer> drives) {
        if (drives.containsKey(cellKey)) {
            drives.put(cellKey, drives.get(cellKey) + 1);
        } else {
            drives.put(cellKey, 1);
        }
    }

    private void putAmount(String cellKey, Double amount, Map<String, Double> amounts) {
        if (amounts.containsKey(cellKey)) {
            amounts.put(cellKey, amount + amounts.get(cellKey));
        } else {
            amounts.put(cellKey, amount);
        }
    }
}
