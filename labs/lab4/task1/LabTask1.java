package lab.task1;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.stream.Stream;

/**
 * Created by filipgulan on 10/06/2017.
 */
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
