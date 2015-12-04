import edu.insight.unlp.nn.example.TwitterSentiMain

/**
 * Created by cnavarro on 24/11/15.
 */
object ScalaSentiment {
  def main (args: Array[String]) {
    val glovePath: String = "resources/embeddings/glove.twitter.27B.50d.txt"
    val datapath: String = "resources/data/twitterSemEval2013.tsv"
    val modelPath: String = "resources/model/learntSentiTwitter.model"

    val sentimenter: TwitterSentiMain = new TwitterSentiMain
    sentimenter.loadModel(modelPath, glovePath, datapath)

    //String sentence = "They may have a SuperBowl in Dallas, but Dallas ain't winning a SuperBowl. Not with that quarterback and owner.";
    val sentence: String = "bad."
    System.out.println(sentimenter.classify(sentence))

  }
}
