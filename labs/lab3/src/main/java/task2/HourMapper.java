package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.RecordParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by filipgulan on 15/04/2017.
 */
public class HourMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        RecordParser parser = new RecordParser();
        try {
            parser.parse(value.toString());
            if (parser.getHour() >= 0) {
                context.write(new IntWritable(parser.getHour()), value);
            }
        } catch (Exception e) {
            // Do nothing
        }
    }
}
