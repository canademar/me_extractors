import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.Serializable;

import edu.insight.unlp.nn.NN;
import edu.insight.unlp.nn.common.Sequence;
import edu.insight.unlp.nn.common.nlp.GloveVectors;
import edu.insight.unlp.nn.example.SentiTweetDataset;
import edu.insight.unlp.nn.utils.SerializationUtils;

/**
 * Created by cnavarro on 24/11/15.
 */
public class NUIGSentiment implements Serializable {
    private GloveVectors word2vec;
    private SentiTweetDataset dataset;
    private NN nn;



    public NUIGSentiment(String modelPath, String embeddingPath, String datasetPath) {
        try {
            this.nn = (NN)SerializationUtils.readObject(new FileInputStream(new File(modelPath)));
        } catch (FileNotFoundException var5) {
            var5.printStackTrace();
        }

        this.word2vec = new GloveVectors(embeddingPath);
        this.dataset = new SentiTweetDataset(this.word2vec, datasetPath);
    }

    public String classify(String text) {
        Sequence sequence = this.dataset.getSequence(text);
        return this.dataset.classify(this.nn, sequence);
    }
}
