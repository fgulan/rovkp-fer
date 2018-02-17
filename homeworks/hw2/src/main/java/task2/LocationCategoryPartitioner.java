package task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
import util.DEBSRecordParser;

/**
 * Created by filipgulan on 08/04/2017.
 */
public class LocationCategoryPartitioner extends Partitioner<IntWritable, Text> {

    @Override
    public int getPartition(IntWritable category, Text value , int i) {
        DEBSRecordParser parser = new DEBSRecordParser();
        String row = value.toString();
        if (row.startsWith("medallion,")) return 0;
        parser.parse(row);
        Boolean isInCenter = parser.isInCenter();
        switch (category.get()) {
            case 1:
                if (isInCenter) return 0;
                else return 1;
            case 2:
                if (isInCenter) return 2;
                else return 3;
            default:
                if (isInCenter) return 4;
                else return 5;
        }
    }
}
