package lab;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.recommender.ItemBasedRecommender;
import org.apache.mahout.cf.taste.recommender.RecommendedItem;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by filipgulan on 23/05/2017.
 */
public class LabTask1 {

    private static final String MODEL_FILE = "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings.dat";
    private static final String OUTPUT_FILE = "item_similarity.csv";

    public static void main(String[] args) throws IOException, TasteException {
        DataModel model = new FileDataModel(new File(MODEL_FILE), "\\s+");
        ItemSimilarity similarity = new CollaborativeItemSimilarity(model);

        ItemBasedRecommender recommender = new GenericItemBasedRecommender(model, similarity);

        List<RecommendedItem> recommendations = recommender.recommend(23, 100);
        for (RecommendedItem recommendation : recommendations) {
            System.out.println(recommendation);
        }
    }
}
