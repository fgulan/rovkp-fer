package task1;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by filipgulan on 07/04/2017.
 */
public class Task1 {

    private static final String INPUT_PATH = "/user/rovkp/trip_data.csv";
    private static final String OUTPUT_PATH = "/user/rovkp/trip_result";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        long startTime = System.nanoTime();
        Job job = getJob(INPUT_PATH, OUTPUT_PATH, Task1.class);
        job.waitForCompletion(true);
        System.out.println("Total time: " + (System.nanoTime() - startTime)/10e8 + "s");
    }

    public static Job getJob(String inputPath, String outputPath, Class<?> cls)
            throws IOException {
        Job job = Job.getInstance();
        job.setJarByClass(cls);
        job.setJobName("TripTime");
        job.setNumReduceTasks(1);

        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        job.setMapperClass(TripTimeMapper.class);
        job.setCombinerClass(TripTimeReducer.class);
        job.setReducerClass(TripTimeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(TripTimeTuple.class);
        return job;
    }
}
