package task2;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by filipgulan on 21/05/2017.
 */
public class ItemBasedJokeRecommender {

    private static final String SIMILARITY_FILE =
            "/Users/filipgulan/Diplomski/Semester_8/ROVKP/ROVKP_DZ3/item_similarity.csv";
    private static final String MODEL_FILE =
            "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings.dat";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(MODEL_FILE), "\\s+");
        ItemSimilarity similarity = new FileItemSimilarity(new File(SIMILARITY_FILE));
        ItemBasedRecommender recommender = new GenericItemBasedRecommender(model, similarity);

        List<RecommendedItem> recommendations = recommender.recommend(220, 10);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }
}