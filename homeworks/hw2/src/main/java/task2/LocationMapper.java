package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DEBSRecordParser;

import java.io.IOException;

/**
 * Created by filipgulan on 08/04/2017.
 */
public class LocationMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String row = value.toString();
        // First line is CSV header
        if (row.startsWith("medallion,")) return;
        DEBSRecordParser parser = new DEBSRecordParser();
        parser.parse(row);
        // Invalid passenger count
        if (parser.getCategory() == -1) return;
        context.write(new IntWritable(parser.getCategory()), new Text(row));
    }
}
