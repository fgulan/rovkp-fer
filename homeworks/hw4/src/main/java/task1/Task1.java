package task1;

import java.io.File;
import java.io.IOException;
import java.net.Inet4Address;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Stream;
import java.util.stream.Collectors;

/**
 * Created by filipgulan on 03/06/2017.
 */
public class Task1 {

    private static final String INPUT_FOLDER = "/Users/filipgulan/Downloads/sensorscope-monitor";
    private static final String OUTPUT_FILE = "senesorscope-monitor-all.csv";

    public static void main(String[] args) throws IOException {
        File[] files = new File(INPUT_FOLDER).listFiles();
        Stream<String> stream = null;
        for (File file : files) {
            if (file.isFile() && file.getName().endsWith("txt")) {
                if (stream == null) {
                    stream = Files.lines(Paths.get(file.getAbsolutePath()));
                } else {
                    stream = Stream.concat(stream, Files.lines(Paths.get(file.getAbsolutePath())));
                }
            }
        }
//
//        Stream<String> resultStream = stream
//                .map(line -> line.split("\\s+"))
//                .filter(items -> SensorscopeReading.isParsable(items))
//                .map(items -> new SensorscopeReading(items))
//                .parallel()
//                .sorted(Comparator.comparingInt(SensorscopeReading::getTimestamp))
//                .map(reading -> reading.toCSVLine());
//
//        Files.write(Paths.get(OUTPUT_FILE),
//                (Iterable<String>)resultStream::iterator);
    }
}
