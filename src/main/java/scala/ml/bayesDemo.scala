package scala.ml

/**
  * Created by MountainG on 2018/6/14.
  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StringIndexer}
import org.apache.spark.sql.SparkSession
object bayesDemo {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
        .master("local")
      .appName("bayesDemo")
      .getOrCreate()
    // Load the data stored in LIBSVM format as a DataFrame.
//    val data = spark.read.format("libsvm").load("F:\\spark-2.3.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")
    val data = spark.read
    .option("header",true)
    .option("delimiter","|")
    .csv("data/doc_class.dat")
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
    //保存模型
//    model.write.overwrite().save("/tmp/model_bayes")
    //保存pipeline
//    pipeline.save("/tmp/pipeline_bayes")

    //加载模型并测试
//    val sameModel = PipelineModel.load("/tmp/model_bayes")
    model.transform(testData).show()
  }
}

