package scala.job


/**  * Created by MountainG on 2018/7/12.  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object classJob06 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("leture_05")
      .getOrCreate()
    import spark.implicits._
    val userInstallArray = Array("userDate", "userId", "softName")
    val userInstallDF = spark.read.textFile("data/userUseList/000000_0", "data/userUseList/000001_0")
      .map(x => x.split("\\s+"))
      .map(x => (x(0), x(1), x(2)))
      .toDF(userInstallArray: _*)
    userInstallDF.show()
    val userLabelArray = Array("userId", "userDate", "tagValue")
    val userLabelDF = spark.read.textFile("data/userData/000000_0", "data/userData/000001_0")
      .map(x => x.split("\\s+"))
      .map(x => (x(0).trim, x(1), x(2).toDouble))
      .toDF(userLabelArray: _*)
    userLabelDF.show()
    userInstallDF.createOrReplaceTempView("userInstallDFTable")
    userLabelDF.createOrReplaceTempView("userLabelDFTable")

    val characterData = spark.sql("select a.userId,a.softName,(case when b.tagValue > 0.5 then cast(1 as double) else 0 end ) as label from userInstallDFTable a join userLabelDFTable b on a.userId=b.userId")
    characterData.show(100)
    val splitSotfName = udf { (words: String) => words.split("\\.") }
    val data = characterData.select("userId", "softName", "label")
      .withColumn("words", splitSotfName(col("softName")))
    data.show(100)
    //样本划分
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    val countVectorizer = new CountVectorizer()
      .setInputCol("words")
      .setOutputCol("features")
      .setMinTF(2)
      .setVocabSize(100000)
    //构建决策树
    val dt = new DecisionTreeClassifier().
      setLabelCol("label").
      setFeaturesCol("features")
    //训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setNumTrees(10)

    //训练GBDT模型
    val gbt = new GBTClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setMaxIter(10)
    //构建Pipeline
    val pipeline = new Pipeline().
      setStages(Array(countVectorizer,  dt))

    val pipeline1 = new Pipeline().
      setStages(Array(countVectorizer,  rf))

    val pipeline2 = new Pipeline().
      setStages(Array(countVectorizer,  gbt))

    val model = pipeline.fit(trainingData)

    val model1 = pipeline1.fit(trainingData)

    val model2 = pipeline2.fit(trainingData)
    //模型测试
    val predictions = model.transform(testData)
    // 测试结果
    predictions.select("label", "features").show(5)
    // 正确率
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy:"+accuracy)
    //f1
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator1.evaluate(predictions)
    println("f1:"+f1)
    //Precision
    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val precision = evaluator2.evaluate(predictions)
    println("precision:"+precision)
    //recall
    val evaluator3 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val recall = evaluator3.evaluate(predictions)
    println("recall:"+recall)
  }
}
