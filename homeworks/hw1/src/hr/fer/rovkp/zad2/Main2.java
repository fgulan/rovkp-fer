package hr.fer.rovkp.zad2;

import java.io.*;
import java.util.*;

public class Main2 {

    static File source = new File("/Users/filipgulan/ROVKP_DZ1/gutenberg/");
    static File destination = new File("/Users/filipgulan/ROVKP_DZ1/gutenberg_books.txt");

    public static void main(String[] args) throws IOException {
        long startTime = System.nanoTime();
        List<String> lines = collectTextLines(source);

        if (!destination.exists()) {
            destination.createNewFile();
        }
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(destination), "ISO_8859_1"))) {
            lines.forEach(line -> {
                try {
                    writer.write(line);
                } catch (IOException e) {}
            });
        }
        long elapsedTime = System.nanoTime() - startTime;
        System.out.println("Broj redaka: " + lines.size()
                + "\nVrijeme: " + elapsedTime / 10e8 + " sec");
    }

    public static List<String> collectTextLines(File root) throws IOException {
        List<String> lines = new ArrayList();
        if(root == null || root.listFiles() == null){
            return lines;
        }
        for (File entry : root.listFiles()) {
            if (!entry.isFile()) {
                lines.addAll(collectTextLines(entry));
                continue;
            }
            if (!entry.toString().endsWith(".txt")) {
                continue;
            }
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(new FileInputStream(entry), "ISO_8859_1"))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines.add(line + System.lineSeparator());
                }
            }
        }
        return lines;
    }
}

