package scala.job

/**
  * Created by MountainG on 2018/7/27.
  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.ml.clustering.LDA
import scala.io.Source
object classJob08 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("leture_05")
      .getOrCreate()
    val data = spark.read
      .option("header",false)
      .option("delimiter","|")
      .csv("data/data_app")
      .toDF("doc_id1","doc_id2","soure","name","doc_name","country","typename","text")

    data.createOrReplaceTempView("data_app")
    val trainData = spark.sql("select * from data_app where typename=\"Lifestyle\" or typename=\"Communication\"")
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
    //LDA
    val lda = new LDA()
      .setFeaturesCol("wordsTF")
      .setK(20)
      .setMaxIter(100)
    //建立管道
    val pipeline = new Pipeline()
      .setStages(Array(regexTokenizer,filterWords,hashingTF,indexer,lda))
    val model = pipeline.fit(trainData)
//    model.stages.
    model.transform(trainData).show()
  }
}
