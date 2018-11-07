package scala.job

/**  * Created by MountainG on 2018/6/27.  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StringIndexer}
import org.apache.spark.sql.SparkSession
object classJob03 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("classJob3")
      .getOrCreate()
    val data = spark.read
      .option("header",true)
      .option("delimiter","|")
      .csv("data/doc_class.dat")
    println(data.show(100))
    val Array(trainingData, testData) = data.randomSplit(Array(0.7,0.3),seed = 1234L)
    //类别数字化
    val indexer = new StringIndexer()
      .setInputCol("typenameid")
      .setOutputCol("typenameidIndex")
      .setHandleInvalid("skip")
    //分词
    val regexTokenizer  = new RegexTokenizer ()
      .setInputCol("myapp_word_all")
      .setOutputCol("myapp_words")
      .setPattern(",")
    //计算tfidf
    val wordCount = new HashingTF()
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("features")
    //用bayes算法产生bayesModel
    val bayes = new NaiveBayes()
      .setFeaturesCol(wordCount.getOutputCol)
      .setLabelCol(indexer.getOutputCol)
      .setSmoothing(1.0)
      .setModelType("multinomial")
    val pipeline = new Pipeline()
      .setStages(Array(indexer,regexTokenizer,wordCount,bayes))
    val model = pipeline.fit(trainingData)
    model.transform(testData).show()
    //保存模型
    model.write.overwrite().save("/tmp/model_bayes")
    //模型测试
    model.transform(testData).show()
  }
}
