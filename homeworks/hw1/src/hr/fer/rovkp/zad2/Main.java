package hr.fer.rovkp.zad2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class Main {

    static Path source = Paths.get("/Users/filipgulan/ROVKP_DZ1/gutenberg/");
    static Path destination = Paths
            .get("/Users/filipgulan/ROVKP_DZ1/gutenberg_books.txt");

    public static void main(String[] args) throws IOException {
        TextFileVisitor visitor = new TextFileVisitor();
        long startTime = System.nanoTime();
        Files.walkFileTree(source, visitor);
        Files.write(destination, visitor.lines,
                StandardCharsets.ISO_8859_1, StandardOpenOption.CREATE);
        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Broj redaka: " + visitor.lines.size()
                + "\nVrijeme: " + elapsedTime / 10e8 + " sec");
    }
}

