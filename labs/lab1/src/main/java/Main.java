import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.List;

/**
 * Created by filipgulan on 27/03/2017.
 */
public class Main {

    static Path source = Paths.get("/home/rovkp/fgulan/gutenberg/");
    static org.apache.hadoop.fs.Path destination =
            new org.apache.hadoop.fs.Path("/user/rovkp/fgulan/gutenberg_books.txt");

    public static void main(String[] args) throws IOException, URISyntaxException {
        BasicConfigurator.configure(new NullAppender());

        Configuration configuration = new Configuration();
        LocalFileSystem lfs = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://cloudera2:8020"), configuration);

        BufferedWriter bufferedWriter = new BufferedWriter(
                new OutputStreamWriter(hdfs.create(destination)));
        TextFileVisitor visitor = new TextFileVisitor();
        visitor.bufferedWriter = bufferedWriter;
        long startTime = System.nanoTime();
        Files.walkFileTree(source, visitor);

        long elpasedTIme = System.nanoTime() - startTime;
        System.out.println("Broj redaka: " + visitor.linesCount
                + "\nVrijeme: " + elpasedTIme / 10e8 + " sec\n"
                + " Broj datoteka: " + visitor.fileCount);
        lfs.close();
        hdfs.close();
    }
}

class TextFileVisitor extends SimpleFileVisitor<Path> {

    Integer linesCount = 0;
    Integer fileCount = 0;
    BufferedWriter bufferedWriter;

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
            throws IOException {
        if (!file.toString().endsWith(".txt")) {
            return FileVisitResult.CONTINUE;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);
        for (String line : lines) {
            bufferedWriter.write(line);
        }
        linesCount += lines.size();
        fileCount += 1;
        return FileVisitResult.CONTINUE;
    }
}