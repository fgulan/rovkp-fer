package lab;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.RandomUtils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by filipgulan on 26/05/2017.
 */
public class LabTask2 {

    private static final Integer JOKES_COUNT = 150;
    private static final String MODEL_FILE = "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings.dat";
    private static final String INPUT_SIMILARITY = "item_similarity.csv";
    private static final String HYBRID_OUTPUT_FILE = "hybrid_item_similarity.csv";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(MODEL_FILE), "\\s+");
        ItemSimilarity similarity = new CollaborativeItemSimilarity(model);
        ItemSimilarity itemSimilarity = new FileItemSimilarity(new File(INPUT_SIMILARITY));
        double[][] a = normalizeSimilarity(similarity);
        double[][] b = normalizeSimilarity(itemSimilarity);
        double[][] combined = combine(a, 1.0, b);
        storeSimilarityMatrixAsCSV(combined, HYBRID_OUTPUT_FILE);
    }

    private static void storeSimilarityMatrixAsCSV(double[][] similarityMatrix, String path) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {

            for (int i = 0; i < similarityMatrix.length; ++i) {
                for (int j = i + 1; j < similarityMatrix[i].length; ++j) {
                    if (!Double.isNaN(similarityMatrix[i][j])) {
                        String row = (i + 1) + "," + (j + 1) + "," + similarityMatrix[i][j];
                        writer.write(row);
                        if (i == similarityMatrix.length - 1 && j == similarityMatrix[i].length -1) continue;
                        writer.newLine();
                    }
                }
            }
        }
    }

    private static double[][] combine(double[][] matrix1, double a, double[][] matrix2) {
        double b = 1.0 - a;
        double[][] similarityMatrix = new double[JOKES_COUNT][JOKES_COUNT];
        for (int i = 0; i < JOKES_COUNT; i++) {
            for (int j = 0; j < JOKES_COUNT; j++) {
                similarityMatrix[i][j] = a * matrix1[i][j] + b * matrix2[i][j];
            }
        }
        return similarityMatrix;
    }

    private static double scaleInterval(double value,
                                        double minActualInterval, double maxActualInterval,
                                        double minDesiredInterval, double maxDesiredInterval) {
        return ((value - minActualInterval) / (maxActualInterval - minActualInterval))
                * (maxDesiredInterval - minDesiredInterval) + minDesiredInterval;

    }

    private static double[][] normalizeSimilarity(ItemSimilarity similarity) throws TasteException {
        double[][] similarityMatrix = new double[JOKES_COUNT][JOKES_COUNT];
        for (int i = 0; i < JOKES_COUNT; i++) {
            double sum = 0.0;
            double min = Double.MAX_VALUE;
            double max = Double.MIN_VALUE;
            for (int j = 0; j < JOKES_COUNT; j++) {
                double sim = similarity.itemSimilarity(i + 1, j + 1);
                if (!Double.isNaN(sim)) {
                    sum += sim;
                    if (sim < min) {
                        min = sim;
                    }
                    if (sim > max) {
                        max = sim;
                    }
                }
                similarityMatrix[i][j] = sim;
            }
            for (int j = 0; j < JOKES_COUNT; j++) {
                similarityMatrix[i][j] = scaleInterval(similarityMatrix[i][j], min, max, 0.0,1.0);
            }
        }
        return similarityMatrix;
    }
}
