import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.varia.NullAppender;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by filipgulan on 25/03/2017.
 */
public class HadoopLineCounter {

    static java.nio.file.Path source = Paths.get("/Users/filipgulan/ROVKP_DZ1/gutenberg/");
    static Path destination = new Path("/user/rovkp/gutenberg_books.txt");

    public static void main(String[] args) throws IOException, URISyntaxException {
        BasicConfigurator.configure(new NullAppender());

        Configuration configuration = new Configuration();
        LocalFileSystem lfs = LocalFileSystem.getLocal(configuration);
        FileSystem hdfs = FileSystem.get(new URI("hdfs://localhost:9000"), configuration);

        TextFileVisitor visitor = new TextFileVisitor();
        long startTime = System.nanoTime();
        Files.walkFileTree(source, visitor);

        try(BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(hdfs.create(destination)))) {
            for (String line : visitor.lines) {
                bufferedWriter.write(line);
            }
        }

        long elpasedTIme = System.nanoTime() - startTime;
        System.out.println("Broj redaka: " + visitor.lines.size()
                + "\nVrijeme: " + elpasedTIme / 10e8 + " sec");
        lfs.close();
        hdfs.close();
    }
}

class TextFileVisitor extends SimpleFileVisitor<java.nio.file.Path> {

    List<String> lines = new ArrayList();
    Integer fileCount = 0;

    @Override
    public FileVisitResult visitFile(java.nio.file.Path file, BasicFileAttributes attrs) throws IOException {
        if (!file.toString().endsWith(".txt")) {
            return FileVisitResult.CONTINUE;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);
        this.lines.addAll(lines);
        fileCount += 1;
        return FileVisitResult.CONTINUE;
    }
}