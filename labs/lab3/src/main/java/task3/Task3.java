package task3;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import task1.Task1;
import task2.Task2;

import java.io.IOException;

/**
 * Created by filipgulan on 15/04/2017.
 */
public class Task3 {

    private static final String INTERMEDIATE_PATH = "intermediate_fgulan";
    private static final String INPUT_PATH = "/user/rovkp/debs2015full";
    private static final String OUTPUT_PATH = "/user/rovkp/fgulan/task3_result";

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {
        Job filterJob = Task1.getJob(INPUT_PATH, INTERMEDIATE_PATH, Task3.class);

        Integer filterResult = filterJob.waitForCompletion(true) ? 0 : 1;
        if (filterResult != 0) {
            FileSystem.get(filterJob.getConfiguration())
                    .delete(new Path(INTERMEDIATE_PATH), true);
            return;
        }

        Job timeJob = Task2.getJob(INTERMEDIATE_PATH, OUTPUT_PATH, Task3.class);
        timeJob.waitForCompletion(true);

        FileSystem.get(filterJob.getConfiguration())
                .delete(new Path(INTERMEDIATE_PATH), true);
    }
}
