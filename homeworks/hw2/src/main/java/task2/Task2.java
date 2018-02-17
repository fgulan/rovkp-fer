package task2;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by filipgulan on 08/04/2017.
 */
public class Task2 {

    private static final String INPUT_PATH = "/user/rovkp/trip_data.csv";
    private static final String OUTPUT_PATH = "/user/rovkp/location_result";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = getJob(INPUT_PATH, OUTPUT_PATH, Task2.class);
        job.waitForCompletion(true);
    }

    public static Job getJob(String inputPath, String outputPath, Class<?> cls)
            throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(cls);
        job.setJobName("Location");

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(LocationMapper.class);
        job.setPartitionerClass(LocationCategoryPartitioner.class);
        job.setReducerClass(LocationReducer.class);
        job.setNumReduceTasks(6);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        return job;
    }
}
