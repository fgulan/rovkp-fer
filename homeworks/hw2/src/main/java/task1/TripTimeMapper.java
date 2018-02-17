package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.DEBSRecordParser;

import java.io.IOException;

/**
 * Created by filipgulan on 07/04/2017.
 */
public class TripTimeMapper extends Mapper<LongWritable, Text, Text, TripTimeTuple> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // First line is CSV header
        String row = value.toString();
        if (row.startsWith("medallion,")) return;
        DEBSRecordParser parser = new DEBSRecordParser();
        parser.parse(row);
        Integer duration = parser.getDuration();
        context.write(new Text(parser.getMedallion()),
                new TripTimeTuple(duration, duration, duration));
    }
}
