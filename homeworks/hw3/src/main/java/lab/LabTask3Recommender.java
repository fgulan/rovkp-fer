package lab;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by filipgulan on 28/05/2017.
 */
public class LabTask3Recommender {

    private static final String MODEL_FILE = "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings.dat";
    private static final String HYBRID_SIMILARITY_FILE = "hybrid_item_similarity.csv";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(MODEL_FILE), "\\s+");
        ItemSimilarity similarity = new FileItemSimilarity(new File(HYBRID_SIMILARITY_FILE));
        ItemBasedRecommender recommender = new GenericItemBasedRecommender(model, similarity);;


        try (BufferedWriter writer = new BufferedWriter(new FileWriter("recommender-out.txt"))) {
            for (int id = 1; id <10; id++) {
                try {
                    String row = Integer.toString(id) + "\t[";
                    List<RecommendedItem> recommendations = recommender.recommend(id, 10);
                    for (RecommendedItem recommendation : recommendations) {
                        row += Long.toString(recommendation.getItemID()) + ":" + Float.toString(recommendation.getValue()) + ",";
                    }
                    row += "]";
                    writer.write(row);
                    writer.newLine();
                } catch (TasteException excp) {
                    continue;
                }
            }
        }
    }
}
