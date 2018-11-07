package scala.ml

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession

object decision_trees_2x {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().
      master("local").
      appName("decision_trees_2x").
      getOrCreate()

    //1 训练样本准备
    val data = spark.read.format("libsvm").load("F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib\\sample_libsvm_data.txt")
    data.show

    //2 标签进行索引编号
    val labelIndexer = new StringIndexer().
      setInputCol("label").
      setOutputCol("indexedLabel").
      fit(data)
    // 对离散特征进行标记索引，以用来确定哪些特征是离散特征
    // 如果一个特征的值超过4个以上，该特征视为连续特征，否则将会标记得离散特征并进行索引编号 
    val featureIndexer = new VectorIndexer().
      setInputCol("features").
      setOutputCol("indexedFeatures").
      setMaxCategories(4).
      fit(data)

    //3 样本划分
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    //4 训练决策树模型
    val dt = new DecisionTreeClassifier().
      setLabelCol("indexedLabel").
      setFeaturesCol("indexedFeatures")

    //4 训练随机森林模型
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setNumTrees(10)

    //4 训练GBDT模型
    val gbt = new GBTClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures")
      .setMaxIter(10)

    //5 将索引的标签转回原始标签
    val labelConverter = new IndexToString().
      setInputCol("prediction").
      setOutputCol("predictedLabel").
      setLabels(labelIndexer.labels)

    //6 构建Pipeline
    val pipeline1 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, dt, labelConverter))

    val pipeline2 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, rf, labelConverter))

    val pipeline3 = new Pipeline().
      setStages(Array(labelIndexer, featureIndexer, gbt, labelConverter))

    //7 Pipeline开始训练
    val model1 = pipeline1.fit(trainingData)

    val model2 = pipeline2.fit(trainingData)

    val model3 = pipeline3.fit(trainingData)

    //8 模型测试
    val predictions = model1.transform(testData)
    predictions.show(5)

    //8 测试结果
    predictions.select("predictedLabel", "label", "features").show(5)

    //9 分类指标
    // 正确率
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println("accuracy:"+accuracy)
    //f1
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator1.evaluate(predictions)
    println("f1:"+f1)
  //Precision
  val evaluator2 = new MulticlassClassificationEvaluator()
    .setLabelCol("indexedLabel")
    .setPredictionCol("prediction")
    .setMetricName("weightedPrecision")
    val precision = evaluator2.evaluate(predictions)
    println("precision:"+precision)
  //recall
  val evaluator3 = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
   val recall = evaluator3.evaluate(predictions)
   println("recall:"+recall)
   //auc
    val evaluator4 = new BinaryClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")
    val auc = evaluator4.evaluate(predictions)
    println("auc:"+ auc)

  }

}