package scala.job

/**  * Created by MountainG on 2018/6/27.  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, RegexTokenizer, StopWordsRemover, StringIndexer}
import org.apache.spark.sql.SparkSession

import scala.io.Source
object classJob04 {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("classJob4")
      .getOrCreate()
    val data = spark.read
      .option("header",false)
      .option("delimiter","|")
      .csv("data/data_app").toDF("doc_id1","doc_id2","soure","name","doc_name","country","typename","text")
    val Array(trainingData, testData) = data.randomSplit(Array(0.7,0.3),seed = 1234L)
    //文档清洗
    val regexTokenizer = new RegexTokenizer()
      .setInputCol("text")
      .setOutputCol("FilterText")
      .setPattern("[^a-zA-Z]")
    //去停用词
    var stopwords = spark.read
      .textFile("data/stopWords.txt").toDF("stopwords")
    val stopList = Source.fromFile("data/stopWords.txt").getLines().toArray
    println(stopList)
    val filterWords = new StopWordsRemover()
      .setInputCol(regexTokenizer.getOutputCol)
      .setOutputCol("FilterWords")
      .setStopWords(stopList)

    //提取关键字TFIDF
    val hashingTF = new HashingTF()
      .setInputCol(filterWords.getOutputCol)
      .setOutputCol("wordsTF")
      .setNumFeatures(10000)
    //类别数字化
    val indexer = new StringIndexer()
      .setInputCol("typename")
      .setOutputCol("typenameIndex")
      .setHandleInvalid("skip")
    //贝叶斯分类器
    val bayes = new NaiveBayes()
      .setFeaturesCol(hashingTF.getOutputCol)
      .setLabelCol(indexer.getOutputCol)
      .setSmoothing(1.0)
      .setModelType("multinomial")
    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer,filterWords,hashingTF,indexer,bayes))
    val model = pipeline.fit(trainingData)
    //模型测试
    model.transform(testData).show()
  }
}
