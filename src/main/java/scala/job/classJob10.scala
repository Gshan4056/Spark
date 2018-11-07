package scala.job

/**
  * Created by 91926 on 2018/8/7.
  */
import org.apache.log4j.{ Level, Logger }
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vectors, DenseVector, Vector }
import scala.collection.mutable.ArrayBuffer
import scala.math._

import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.tree.GradientBoostedTrees
import org.apache.spark.mllib.tree.loss
import org.apache.spark.mllib.tree.impurity.{ Variance, Entropy, Gini, Impurity }
import org.apache.spark.mllib.evaluation.{ MulticlassMetrics, BinaryClassificationMetrics }

import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.configuration.FeatureType._
import org.apache.spark.mllib.tree.configuration.Strategy
import org.apache.spark.mllib.tree.configuration.Algo.{ Regression, Classification, Algo }
import org.apache.spark.mllib.tree.configuration.QuantileStrategy._

import org.apache.spark.mllib.tree.model.DecisionTreeModel
import org.apache.spark.mllib.tree.model.Node
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel

import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{ FileSystem, Path }
import java.net.URI
import java.io.{ ObjectOutputStream, ObjectInputStream }
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

import org.apache.spark.ml.regression.{RandomForestRegressionModel, RandomForestRegressor}

object classJob10 {

  def main(args: Array[String]): Unit = {

    // 接收参数
    val sampleDate = args(0)
    val out_path = args(1)

    println("sampleDate： " + sampleDate)
    println("out_path： " + out_path)

    //0 构建Spark对象
    val conf = new SparkConf().setAppName("DT_Train")
    val sc = new SparkContext(conf)
    val HiveContext = new HiveContext(sc)
    val spark = SparkSession.builder()
      .config(conf)
      .master("local")
      .appName("classJob10")
      .getOrCreate()
    //1 训练样本日志表
    val sql1 = "select if(click > 0, 1.0, 0.0) label, detail, task_id from t_train_sample where ds = " + sampleDate
    val data1 = HiveContext.sql(sql1).rdd

    //创建dataFrame
    val schema = StructType(List(StructField("label", DoubleType, true),StructField("detail", StringType, true),StructField("task_id", IntegerType, true)))
    val rowRDD = data1.map(p => Row(p(0), p(1), p(2)))
    val data = spark.createDataFrame(rowRDD, schema)


    //训练集和测试集的划分
    val Array(trainData,testData) = data.randomSplit(Array(0.7,0.3))

    //模型训练
    val rf = new RandomForestRegressor()
      .setLabelCol("label")
      .setFeaturesCol("indexedFeatures")

    val model = rf.fit(trainData)

   //模型测试
    val prediction = model.transform(testData)

    prediction.show()

    //模型保存
    model.save(out_path + "/rf_model")
  }
}
