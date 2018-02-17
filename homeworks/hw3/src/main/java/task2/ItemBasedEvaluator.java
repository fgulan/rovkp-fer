package task2;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.recommender.GenericItemBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.file.FileItemSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.similarity.ItemSimilarity;
import org.apache.mahout.common.RandomUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by filipgulan on 21/05/2017.
 */
public class ItemBasedEvaluator {

    private static final String SIMILARITY_FILE =
            "/Users/filipgulan/Diplomski/Semester_8/ROVKP/ROVKP_DZ3/item_similarity.csv";
    private static final String MODEL_FILE =
            "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings.dat";

    // 4.6146243691285465
    public static void main(String[] args) throws IOException, TasteException {
        RandomUtils.useTestSeed();
        DataModel model = new FileDataModel(new File(MODEL_FILE), "\\s+");
        RecommenderBuilder builder = model1 -> {
            ItemSimilarity similarity = new FileItemSimilarity(new File(SIMILARITY_FILE));
            return new GenericItemBasedRecommender(model1, similarity);
        };
        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(builder, null, model, 0.4, 0.6);
        System.out.println(score);
    }
}
