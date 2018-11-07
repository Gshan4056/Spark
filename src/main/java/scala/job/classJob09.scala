package scala.job

/**
  * Created by 91926 on 2018/8/3.
  */
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{DecisionTreeClassifier, GBTClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object classJob09 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("classJob09")
      .getOrCreate()
    val data_i = spark.read
      .option("header","false")
      .option("delimiter","\t")
      .csv("data/userUseList/000000_0")
      .toDF("userDate", "userId", "softName")
    val data_j = data_i.toDF("userDate", "userId", "softName1")
    val data = data_i.join(data_j,"userId")
    data.show()
    val calScore = udf{(words: String) => 1 }
    val user_data = data.withColumn("score",calScore{col("userId")}).select("softName","softName1","score").toDF("itemidI","itemidJ","score")
    //    物品:物品:频次
    val user_ds = user_data.groupBy("itemidI", "itemidJ").agg(sum("score").as("sumIJ")).limit(100)
    //  对角矩阵
    val user_ds1 = user_ds.where("itemidI = itemidJ")

    //  非对角矩阵
    val user_ds2 = user_ds.filter("itemidI != itemidJ")

    //  计算同现相似度（物品1，物品2，同现频次）
    val user_ds3 = user_ds2.join(user_ds1.withColumnRenamed("sumIJ", "sumJ").select("itemidJ", "sumJ"), "itemidJ")

    val user_ds4 = user_ds3.join(user_ds2.withColumnRenamed("sumIJ", "sumI").select("itemidI", "sumI"), "itemidI")

    val user_ds5 = user_ds4.withColumn("result", col("sumIJ") / sqrt(col("sumI") * col("sumJ")))
    user_ds5.show()
  }
}
