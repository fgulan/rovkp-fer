package lab.task2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Created by filipgulan on 10/06/2017.
 */
public class LabTask2 {

    private static final String INPUT_FILE = "/Users/filipgulan/Downloads/DeathRecords.csv";

    public static void main(String[] args)  {
        SparkConf conf = new SparkConf().setAppName("LabTask2");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local");
        }
        JavaSparkContext context = new JavaSparkContext(conf);
        JavaRDD<String> lines = context.textFile(INPUT_FILE).persist(StorageLevel.MEMORY_ONLY());;

        JavaRDD<USDeathRecord> result = lines
                .map(line -> line.split(","))
                .filter(USDeathRecord::isParsable)
                .map(USDeathRecord::new);

//        printDeaths(result, 6, true);
//        printDayOfMostMaleDeaths(result);
//        printTotalCountOfAutopsies(result);
//        printDeathMonthStatistic(result, 45, 65, false);
//        printMarriedMaleDeathDistribution(result, 45, 65);
//        printTotalDeathAccidents(result);
        printDistinctAgesCount(result);
    }

    private static void printDistinctAgesCount(JavaRDD<USDeathRecord> result) {
        long count = result
                .map(USDeathRecord::getAge)
                .distinct()
                .count();

        List list = result.map(USDeathRecord::getDayOfWeekOfDeath).distinct().collect();

        list.forEach(System.out::println);
        System.out.println("Number of distinct ages: " + list);
    }

    private static void printTotalDeathAccidents(JavaRDD<USDeathRecord> result) {
        long count = result
                .filter(USDeathRecord::isAccident)
                .count();
        System.out.println("Total deaths by accident: " + count);
    }

    private static void printMarriedMaleDeathDistribution(JavaRDD<USDeathRecord> result, int bottomYear, int topYear) {
        JavaRDD<USDeathRecord> maleResult = result
                .filter(USDeathRecord::isMale)
                .filter(record -> record.getAge() >= bottomYear && record.getAge() <= topYear);

        Map<Integer, Integer> allRecords = maleResult
                .mapToPair(record -> new Tuple2<>(record.getMonthOfDeath(), 1))
                .reduceByKey((x, y) -> x + y)
                .collectAsMap();

        List<Tuple2<Integer, Double>> marriedRecords = maleResult
                .filter(USDeathRecord::isMarried)
                .mapToPair(record -> new Tuple2<>(record.getMonthOfDeath(), 1))
                .reduceByKey((x, y) -> x + y)
                .sortByKey()
                .map(tuple -> new Tuple2<>(tuple._1,
                        new Double(tuple._2) /
                                new Double(allRecords.getOrDefault(tuple._1, tuple._2))))
                .collect();

        marriedRecords.forEach(record -> {
            System.out.println(record._1 + " - " + record._2);
        });
    }

    private static void printDeathMonthStatistic(JavaRDD<USDeathRecord> result, int bottomYear, int topYear, boolean isFemale) {
        List<Tuple2<Integer, Integer>> records = result
                .filter(record -> record.isFemale() == isFemale)
                .filter(record -> record.getAge() >= bottomYear && record.getAge() <= topYear)
                .mapToPair(record -> new Tuple2<>(record.getMonthOfDeath(), 1))
                .reduceByKey((x, y) -> x + y)
                .sortByKey()
                .collect();
        records.forEach(record -> {
            System.out.println(record._1 + " - " + record._2);
        });

    }

    private static void printTotalCountOfAutopsies(JavaRDD<USDeathRecord> result) {
        long count = result
                .filter(USDeathRecord::autopsyDone)
                .count();
        System.out.println("Number of autopsies: " + count);
    }

    private static void printDayOfMostMaleDeaths(JavaRDD<USDeathRecord> result) {
        int dayOfWeek = result
                .filter(USDeathRecord::isMale)
                .filter(record -> record.getAge() > 50)
                .mapToPair(record -> new Tuple2<>(record.getDayOfWeekOfDeath(), 1))
                .reduceByKey((x, y) -> x + y)
                .mapToPair(Tuple2::swap).sortByKey(false).mapToPair(Tuple2::swap)
                .first()._1;
        System.out.println("Day with most male deaths: " + dayOfWeek);
    }

    private static void printDeaths(JavaRDD<USDeathRecord> result, int monthOfDeath, boolean isFemale) {
        long count = result
                .filter(record -> record.isFemale() == isFemale)
                .filter(record -> record.getMonthOfDeath() == monthOfDeath)
                .count();
        System.out.println("Number of deaths of " + (isFemale ? "female" : "male")
                + " in " + monthOfDeath + ". month is " + count);
    }
}
