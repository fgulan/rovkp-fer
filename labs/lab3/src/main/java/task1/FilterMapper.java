package task1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import util.RecordParser;

import java.io.IOException;
import java.text.ParseException;

/**
 * Created by filipgulan on 13/04/2017.
 */
public class FilterMapper extends Mapper<LongWritable, Text, NullWritable, Text> {

    private RecordParser parser = new RecordParser();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        try {
            parser.parse(value.toString());
            if (parser.getTotalAmount() >= 0 && parser.isInArea()) {
                context.write(NullWritable.get(), value);
            }
        } catch (Exception e) {
            // Do nothing for now
        }
    }
}
