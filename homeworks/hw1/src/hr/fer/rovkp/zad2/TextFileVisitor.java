package hr.fer.rovkp.zad2;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by filipgulan on 25/03/2017.
 */
public class TextFileVisitor extends SimpleFileVisitor<Path> {

    List<String> lines = new ArrayList();
    Integer fileCount = 0;

    @Override
    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
        if (!file.toString().endsWith(".txt")) {
            return FileVisitResult.CONTINUE;
        }
        List<String> lines = Files.readAllLines(file, StandardCharsets.ISO_8859_1);
        this.lines.addAll(lines);
        fileCount += 1;
        return FileVisitResult.CONTINUE;
    }
}
