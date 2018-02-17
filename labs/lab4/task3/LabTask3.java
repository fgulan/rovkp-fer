package lab.task3;

import lab.task1.PollutionReading;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;
import task1.SensorscopeReading;
import task3.SensorStreamGenerator;

import java.util.NoSuchElementException;

/**
 * Created by filipgulan on 10/06/2017.
 */
public class LabTask3 {
    private static final String OUTPUT = "task3/output";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LabTask3");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }

        JavaStreamingContext context = new JavaStreamingContext(conf, Durations.seconds(5));
        JavaDStream<String> records = context.socketTextStream("localhost",
                SensorStreamGenerator.PORT);

        JavaPairDStream<String, Integer> result = records
                .map(line -> line.split(","))
                .filter(PollutionReading::isParsable)
                .map(PollutionReading::new)
                .mapToPair((reading) -> new Tuple2<>(reading.getStationID(), reading.getOzone()))
                .reduceByKeyAndWindow(Math::max, Durations.seconds(45), Durations.seconds(15));

        result.dstream().saveAsTextFiles(OUTPUT, "txt");
        context.start();
        context.awaitTermination();
    }
}
