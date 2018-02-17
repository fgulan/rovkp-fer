package task3;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import task1.Task1;
import task2.Task2;

import java.io.IOException;

/**
 * Created by filipgulan on 08/04/2017.
 */
public class Task3 {

    private static final String INTERMEDIATE_PATH = "intermediate";
    private static final String INPUT_PATH = "/user/rovkp/trip_data.csv";
    private static final String OUTPUT_PATH = "/user/rovkp/jr/joined_result";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job partitionJob = Task2.getJob(INPUT_PATH, INTERMEDIATE_PATH, Task3.class);

        Integer partitionResult = partitionJob.waitForCompletion(true) ? 0 : 1;
        if (partitionResult != 0) {
            FileSystem.get(partitionJob.getConfiguration())
                    .delete(new Path(INTERMEDIATE_PATH), true);
            return;
        }
        for (int i = 0; i < 6; i++) {
            Job timeJob = Task1.getJob(INTERMEDIATE_PATH + "/part-r-0000"+i,
                    OUTPUT_PATH+i, Task3.class);
            timeJob.waitForCompletion(true);
            FileSystem.get(timeJob.getConfiguration())
                    .delete(new Path(INTERMEDIATE_PATH + "/part-r-0000"+i), true);
        }
        FileSystem.get(partitionJob.getConfiguration())
                .delete(new Path(INTERMEDIATE_PATH), true);
    }
}
