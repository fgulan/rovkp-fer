package task2;

import org.apache.mahout.cf.taste.common.TasteException;
import org.apache.mahout.cf.taste.eval.RecommenderBuilder;
import org.apache.mahout.cf.taste.eval.RecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.eval.RMSRecommenderEvaluator;
import org.apache.mahout.cf.taste.impl.model.file.FileDataModel;
import org.apache.mahout.cf.taste.impl.neighborhood.ThresholdUserNeighborhood;
import org.apache.mahout.cf.taste.impl.recommender.GenericUserBasedRecommender;
import org.apache.mahout.cf.taste.impl.similarity.LogLikelihoodSimilarity;
import org.apache.mahout.cf.taste.model.DataModel;
import org.apache.mahout.cf.taste.neighborhood.UserNeighborhood;
import org.apache.mahout.cf.taste.similarity.UserSimilarity;
import org.apache.mahout.common.RandomUtils;

import java.io.File;
import java.io.IOException;

/**
 * Created by filipgulan on 21/05/2017.
 */
public class UserBasedEvaluator {
//4.938738489279313 pearson

    private static final String MODEL_FILE =
            "/Users/filipgulan/Downloads/jester_dataset_2/jester_ratings1.csv";

    public static void main(String[] args) throws IOException, TasteException {
        RandomUtils.useTestSeed();
        DataModel model = new FileDataModel(new File(MODEL_FILE));
        RecommenderBuilder builder = model1 -> {
//            UserSimilarity similarity = new PearsonCorrelationSimilarity(model1);
            UserSimilarity similarity = new LogLikelihoodSimilarity(model);
            UserNeighborhood neighborhood = new ThresholdUserNeighborhood(0.1, similarity, model1);
            return new GenericUserBasedRecommender(model1, neighborhood, similarity);
        };

        RecommenderEvaluator recEvaluator = new RMSRecommenderEvaluator();
        double score = recEvaluator.evaluate(builder, null, model, 0.4, 0.6);
        System.out.println(score);
    }
}
