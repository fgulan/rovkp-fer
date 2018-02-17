package task1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by filipgulan on 13/04/2017.
 */
public class Task1 {

    private static final String INPUT_PATH = "/user/rovkp/sorted_data.csv";
    private static final String OUTPUT_PATH = "/user/rovkp/lab2/task1_result";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job job = getJob(INPUT_PATH, OUTPUT_PATH, Task1.class);
        job.waitForCompletion(true);
    }

    public static Job getJob(String inputPath, String outputPath, Class<?> cls)
            throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(cls);
        job.setJobName("FilteredTrips");
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(FilterMapper.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        return job;
    }
}
