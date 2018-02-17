package task2;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.NearestNUserNeighborhood;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.impl.similarity.PearsonCorrelationSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.recommender.UserBasedRecommender;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Created by filipgulan on 21/05/2017.
 */
public class UserBasedJokesRecommender {

    private static final String MODEL_FILE =
            "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings1.csv";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(MODEL_FILE));
        UserSimilarity similarity = new PearsonCorrelationSimilarity(model);
        UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model);
        UserBasedRecommender recommender = new GenericUserBasedRecommender(model, neighborhood, similarity);;

        List<RecommendedItem> recommendations = recommender.recommend(220, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("out.txt"))) {
            for (int id = 1; id <101; id++) {
                try {
                    String row = Integer.toString(id) + "\t[";

                    recommendations = recommender.recommend(id, 10);
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
