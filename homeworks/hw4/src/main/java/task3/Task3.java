package task3;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import task1.SensorscopeReading;

import java.util.NoSuchElementException;

/**
 * Created by filipgulan on 05/06/2017.
 */
public class Task3 {

    private static final String OUTPUT = "output";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Task3");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> records = jssc.socketTextStream("localhost",
                SensorStreamGenerator.PORT);

        JavaPairDStream<Integer, Double> result = records
                .map(line -> line.split("\\s+"))
                .filter(SensorscopeReading::isParsable)
                .map(SensorscopeReading::new)
                .mapToPair(record -> new Tuple2<>(record.getStationID(), record.getSolarCurrent()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(60), Durations.seconds(10));

        result.dstream().saveAsTextFiles(OUTPUT, "txt");
        jssc.start();
        jssc.awaitTermination();
    }
}
