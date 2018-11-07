package scala.ml

/**
  * Created by 91926 on 2018/6/12.
  */
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.SparkSession
object featureTest {
  def main(args: Array[String]): Unit = {
    // Input data: Each row is a bag of words from a sentence or document.
    val spark = SparkSession.builder
      .master("local")
      .appName("example")
      .getOrCreate()



    val dataFrame = spark.read.format("libsvm").load("F:\\spark-2.3.0-bin-hadoop2.7/data/mllib/sample_libsvm_data.txt")

    val scaler = new StandardScaler()
      .setInputCol("features")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    // Compute summary statistics by fitting the StandardScaler.
    val scalerModel = scaler.fit(dataFrame)

    // Normalize each feature to have unit standard deviation.
    val scaledData = scalerModel.transform(dataFrame)
    scaledData.show()
  }
}


