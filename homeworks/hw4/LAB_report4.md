# Raspodijeljena obrada velike količine podataka

## 4. Laboratorijska vježba

##### Filip Gulan - JMBAG: 0036479428

### 1. zadatak

##### LabTask1.java
~~~java
public class LabTask1 {

    private static final String INPUT_FOLDER = "/Users/filipgulan/Downloads/pollutionData";
    private static final String OUTPUT_FILE = "pollutionData-all.csv";

    public static void main(String[] args) throws IOException {
        File[] files = new File(INPUT_FOLDER).listFiles();
        Stream<String> stream = null;
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith("csv")) {
                if (stream == null) {
                    stream = Files.lines(Paths.get(file.getAbsolutePath()));
                } else {
                    stream = Stream.concat(stream, Files.lines(Paths.get(file.getAbsolutePath())));
                }
            }
        }

        Stream<String> resultStream = stream
                .map(line -> line.split(","))
                .filter(PollutionReading::isParsable)
                .map(PollutionReading::new)
                .sorted(Comparator.comparing(PollutionReading::getTimestamp))
                .map(PollutionReading::toString);

        Files.write(Paths.get(OUTPUT_FILE),
                (Iterable<String>)resultStream::iterator);
    }
}
~~~

##### PollutionReading.java

~~~java
public class PollutionReading {
   
    private String[] items;
    private Integer ozone;

    public PollutionReading(String[] items) {
        this.items = items;
        this.ozone = Integer.parseInt(items[0]);
    }

    public String getTimestamp() {
        return items[7];
    }

    public String getStationID() {
        return items[5] + "," + items[6];
    }

    public Integer getOzone() {
        return ozone;
    }

    @Override
    public String toString() {
        return String.join(",", items);
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[0]);
            Integer.parseInt(items[1]);
            Integer.parseInt(items[2]);
            Integer.parseInt(items[3]);
            Integer.parseInt(items[4]);
            Double.parseDouble(items[5]);
            Double.parseDouble(items[6]);
            return true;
        } catch (NumberFormatException exception) {
            return false;
        }
    }
}
~~~

### 2. zadatak

1. Broj ženskih osoba umrlih u lipnju: 100654.
2. Najviše muških osoba starijih od 50 godina je umrlo u četvrtak (dobiven je broj 4 - nigdje nije navedeno koji se dan uzima kao prvi dan tjedna).
3. Broj autopsija: 203681
4. Kretanje smrtnosti muškaraca u dobi od 45 do 65 godina po mjesecima:

  * 1. => 32807
  * 2. => 28107
  * 3. => 29582
  * 4. => 28132
  * 5. => 28191
  * 6. => 27606
  * 7. => 28404
  * 8. => 27940
  * 9. => 27099
  * 10. => 28726
  * 11. => 28431
  * 12. => 30336

5. Kretanje postotka umrlih oženjenih muškaraca u dobi od 45 do 65 godina po mjesecima:

  * 1. => 0.4348157405431768
  * 2. => 0.44145586508698903
  * 3. => 0.4445270772767223
  * 4. => 0.44739087160528934
  * 5. => 0.4456741513248909
  * 6. => 0.4438890096355865
  * 7. => 0.4403605126038586
  * 8. => 0.44355762347888333
  * 9. => 0.4406066644525628
  * 10. => 0.44280442804428044
  * 11. => 0.4458865323062854
  * 12. => 0.43492879746835444

6. Ukupan broj umrlih u nesreći: 132684.
7. Broj različitih godina: 117. 

#### Izvorni kod

##### LabTask2.java

~~~java
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

        printDeaths(result, 6, true);
        printDayOfMostMaleDeaths(result);
        printTotalCountOfAutopsies(result);
        printDeathMonthStatistic(result, 45, 65, false);
        printMarriedMaleDeathDistribution(result, 45, 65);
        printTotalDeathAccidents(result);
        printDistinctAgesCount(result);
    }

    private static void printDistinctAgesCount(JavaRDD<USDeathRecord> result) {
        long count = result
                .map(USDeathRecord::getAge)
                .distinct()
                .count();
        System.out.println("Number of distinct ages: " + count);
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
~~~

##### USDeathRecord.java

~~~java
public class USDeathRecord {    
            
    private final int monthOfDeath, age, dayOfWeekOfDeath, mannerOfDeath;
    private final String sex, maritalStatus, autopsy;

    public USDeathRecord(String[] items) throws NumberFormatException {
        monthOfDeath = Integer.parseInt(items[5]);
        sex = items[6];
        age = Integer.parseInt(items[8]);
        maritalStatus = items[15];
        dayOfWeekOfDeath = Integer.parseInt(items[16]);
        mannerOfDeath = Integer.parseInt(items[19]);
        autopsy = items[21];
    }

    public int getMonthOfDeath() {
        return monthOfDeath;
    }

    public String getGender() {
        return sex;
    }

    public int getAge() {
        return age;
    }

    public String getMaritalStatus() {
        return maritalStatus;
    }

    public int getDayOfWeekOfDeath() {
        return dayOfWeekOfDeath;
    }

    public int getMannerOfDeath() {
        return mannerOfDeath;
    }

    public String getAutopsy() {
        return autopsy;
    }

    public boolean isMale() {
        return sex.equals("M");
    }

    public boolean isFemale() {
        return sex.equals("F");
    }

    public boolean autopsyDone() {
        return autopsy.equals("Y");
    }

    public boolean isMarried() {
        return maritalStatus.equals("M");
    }

    public boolean isAccident() {
        return mannerOfDeath == 1;
    }

    public static boolean isParsable(String[] items) {
        try {
            Integer.parseInt(items[5]);
            Integer.parseInt(items[8]);
            Integer.parseInt(items[16]);
            Integer.parseInt(items[19]);
            return true;
        } catch (NumberFormatException ex) {
            return false;
        }
    }
}
~~~

### 3. zadatak

##### LabTask3.java

~~~java
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
~~~