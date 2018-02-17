import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.*;

/**
 * Created by filipgulan on 27/03/2017.
 */
public class Main {

    static Path dataFilePath = new Path("/user/rovkp/fgulan/ocitanja.bin");
    public static void main(String[] args) throws IOException,
            URISyntaxException, IllegalAccessException, InstantiationException {
        BasicConfigurator.configure(new NullAppender());
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://cloudera2:8020");

        IntWritable key = new IntWritable();
        FloatWritable value = new FloatWritable();
        SequenceFile.Writer writer = SequenceFile.createWriter(configuration,
                SequenceFile.Writer.file(dataFilePath),
                SequenceFile.Writer.keyClass(key.getClass()),
                SequenceFile.Writer.valueClass(value.getClass()));

        Random random = new Random();
        for (int i = 0; i < 100000; i++) {
            key.set(random.nextInt(100) + 1);
            value.set(random.nextFloat() * 99.99f);
            writer.append(key, value);
        }
        writer.close();

        Map<Integer, Float> sensorSum = new HashMap<>();
        Map<Integer, Integer> sensorCount = new HashMap<>();

        SequenceFile.Reader reader = new SequenceFile.Reader(configuration,
                SequenceFile.Reader.file(dataFilePath));
        while (reader.next(key, value)) {
            Integer dictKey = key.get();

            if (sensorCount.containsKey(dictKey)) {
                sensorSum.put(dictKey, value.get() + sensorSum.get(dictKey));
                sensorCount.put(dictKey, 1 + sensorCount.get(dictKey));
            } else {
                sensorSum.put(dictKey, value.get());
                sensorCount.put(dictKey, 1);
            }
        }
        for (Map.Entry<Integer, Integer> entry : sensorCount.entrySet()) {
            System.out.println("Senzor " + entry.getKey() + ": "
                    + sensorSum.get(entry.getKey()) / entry.getValue());
        }
    }
}
