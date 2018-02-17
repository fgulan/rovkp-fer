import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.*;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.apache.mahout.cf.taste.common.TasteException;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

/**
 * Created by filipgulan on 14/05/2017.
 */
public class Task1 {

    // 1. 8358
    // 2. 1, 87, 0.44775397
    // Da, obje sadrže riječ doktor i slične medicinske termine višew puta
    // Da, po prethodno vidjenom primjeru trebala bi

    private static final String INPUT_FILE = "/Users/filipgulan/Downloads/jester_dataset_2/jester_items.dat";
    private static final String OUTPUT_FILE = "item_similarity.csv";

    private static StandardAnalyzer analyzer = new StandardAnalyzer();
    private static Directory index = new RAMDirectory();

    public static void main(String[] args) throws IOException, ParseException {
        List<String> lines =  Files.readAllLines(Paths.get(INPUT_FILE));
        Map<Integer, String> jokePairs = parseInput(lines);
        createLuceneDocuments(jokePairs);
        float[][] similarityMatrix = queryDocs(jokePairs);
        similarityMatrix = normalizeSimilarityMatrix(similarityMatrix);
        storeSimilarityMatrixAsCSV(similarityMatrix, OUTPUT_FILE);
    }

    private static void storeSimilarityMatrixAsCSV(float[][] similarityMatrix, String path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            for (int i = 0; i < similarityMatrix.length; ++i) {
                for (int j = i + 1; j < similarityMatrix[i].length; ++j) {
                    if (similarityMatrix[i][j] > 0) {
                        String row = (i + 1) + "," + (j + 1) + "," + similarityMatrix[i][j];
                        writer.write(row);
                        if (i == similarityMatrix.length - 1 && j == similarityMatrix[i].length -1) continue;
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static float[][] normalizeSimilarityMatrix(float[][] similarityMatrix) {
        for (Integer i = 0; i < similarityMatrix.length; i++) {
            Float max = similarityMatrix[i][i];
            for (Integer j = 0; j < similarityMatrix[i].length; j++) {
                similarityMatrix[i][j] /= max;
            }
        }

        for (int i = 0; i < similarityMatrix.length; i ++) {
            for (int j = 0; j <= i; j++) {
                similarityMatrix[j][i] = similarityMatrix[i][j] =
                        (similarityMatrix[i][j] + similarityMatrix[j][i]) / 2.0f;

            }
        }
        return similarityMatrix;
    }

    private static float[][] queryDocs(Map<Integer, String> jokePairs) throws ParseException, IOException {
        Integer documentsCount = jokePairs.size();
        float[][] similarity = new float[documentsCount][documentsCount];
        IndexReader reader = DirectoryReader.open(index);
        IndexSearcher searcher = new IndexSearcher(reader);

        Integer currentDocumentIndex = 0;
        for (Map.Entry<Integer, String> entry : jokePairs.entrySet()) {
            Query query = new QueryParser("text", analyzer).parse(QueryParser.escape(entry.getValue()));
            for (ScoreDoc hit : searcher.search(query, documentsCount).scoreDocs) {
                Document document = reader.document(hit.doc);
                Integer documentID = Integer.parseInt(document.get("ID"));
                Integer documentIndex = documentID - 1;
                similarity[currentDocumentIndex][documentIndex] = hit.score;
            }
            currentDocumentIndex++;
        }
        reader.close();
        return similarity;
    }

    private static void createLuceneDocuments(Map<Integer, String> jokePairs) throws IOException {
        FieldType idFieldType = new FieldType();
        idFieldType.setStored(true);
        idFieldType.setTokenized(false);
        idFieldType.setIndexOptions(IndexOptions.NONE);

        FieldType jokeFieldType = new FieldType();
        jokeFieldType.setStored(true);
        jokeFieldType.setTokenized(true);
        jokeFieldType.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);

        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        IndexWriter w = new IndexWriter(index, config);
        for (Map.Entry<Integer, String> entry : jokePairs.entrySet()) {
            Document document = new Document();
            document.add(new Field("ID", entry.getKey().toString(), idFieldType));
            document.add(new Field("text", entry.getValue(), jokeFieldType));
            w.addDocument(document);
        }
        w.close();
    }

    private static Map<Integer, String> parseInput(List<String> lines) {
        Iterator<String> iterator = lines.iterator();
        Map<Integer, String> jokes = new HashMap<>();

        while (iterator.hasNext()) {
            Integer id = Integer.parseInt(iterator.next().replace(":", "").trim());
            String joke = "";
            while (iterator.hasNext()) {
                String line = iterator.next();
                if (line.isEmpty()) {
                    break;
                }
                joke += line;
            }
            joke = StringEscapeUtils.unescapeXml(joke.toLowerCase().replaceAll("\\<.*?\\>", ""));
            jokes.put(id, joke);
        }
        return jokes;
    }
}
