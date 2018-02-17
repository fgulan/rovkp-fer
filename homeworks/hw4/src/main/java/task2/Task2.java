package task2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Int;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by filipgulan on 04/06/2017.
 */
public class Task2 {

    private static final String INPUT_FILE = "/Users/filipgulan/Downloads/StateNames.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("LabTask2");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile(INPUT_FILE);

        JavaRDD<USBabyNameRecord> result = lines
                .map(line -> line.split(","))
                .filter(items -> USBabyNameRecord.isParsable(items))
                .map(items -> new USBabyNameRecord(items))
                .cache();
        printUnpopularFemaleName(result);
        printMostPpularMaleNames(result);
        printBestCountry(result, 1946);
        printYearStatistic(result);
        printNameStatistic(result, "Mary");
        printSum(result);
        printDistinctNames(result);
    }

    private static void printDistinctNames(JavaRDD<USBabyNameRecord> result) {
        Long sum = result.map(record -> record.getName()).distinct().count();
        System.out.println("Distinct names: " + sum);
    }

    private static void printSum(JavaRDD<USBabyNameRecord> result) {
        Integer sum = result.map(record -> record.getCount()).reduce((x, y) -> x + y);
        System.out.println("Sum: " + sum);
    }

    private static void printNameStatistic(JavaRDD<USBabyNameRecord> result, String name) {
        Map<Integer, Integer> records = result
                .mapToPair(record -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).collectAsMap();

        List<Tuple2<Integer, Double>> nameRecords = result
                .filter(record -> record.getName().equals(name))
                .mapToPair(record -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .map(tuple -> new Tuple2<>(tuple._1,
                        new Double(tuple._2) /
                                new Double(records.getOrDefault(tuple._1, tuple._2)))).collect();

        nameRecords.forEach(record -> {
            System.out.println(record._1 + "," + record._2);
        });
    }

    private static void printYearStatistic(JavaRDD<USBabyNameRecord> result) {
        List<Tuple2<Integer, Integer>> records = result
                .mapToPair(record -> new Tuple2<>(record.getYear(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).collect();
        records.forEach(record -> {
            System.out.println(record._1 + "," + record._2);
        });
    }

    private static void printBestCountry(JavaRDD<USBabyNameRecord> result, int year) {
        JavaPairRDD records = result
                .filter(record -> record.getYear() == year)
                .mapToPair(record -> new Tuple2<>(record.getState(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap());
        System.out.println("State with most children: " + records.first()._1);

    }

    private static void printMostPpularMaleNames(JavaRDD<USBabyNameRecord> result) {
        List<Tuple2<String, Integer>> records = result
                .filter(record -> !record.isFemale())
                .mapToPair(record -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> x.swap()).sortByKey(false).mapToPair(x -> x.swap()).take(10);
        records.forEach(record -> {
            System.out.println(record._1 + " - " + record._2);
        });
    }

    private static void printUnpopularFemaleName(JavaRDD<USBabyNameRecord> result) {
        JavaPairRDD rdd = result
                .filter(record -> record.isFemale())
                .mapToPair(record -> new Tuple2<>(record.getName(), record.getCount()))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(x -> x.swap()).sortByKey(true).mapToPair(x -> x.swap());
        System.out.println("Most unpopular female name: " + rdd.first()._1);
    }
}

