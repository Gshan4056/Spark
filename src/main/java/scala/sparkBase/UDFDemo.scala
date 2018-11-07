package scala.sparkBase

/**
  * Created by 91926 on 2018/7/28.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{udf,col}
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
object UDFDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("udf")
      .getOrCreate()
    import org.apache.spark.sql.functions._
    // define a case class
    case class DeviceData (id: Int, device: String)
    // create some sample data
    val data = spark.read.json("data/temperatures.json")
    val convert = udf{(temper:Double) => ((temper* 9.0/5.0 ) + 32.0)}
    val resultData = data.select("avgHigh","avgLow","city")
      .withColumn("avgHighF",convert({col("avgHigh")}))
      .withColumn("avgLowF",convert({col("avgLow")}))
    resultData.show()
//    spark.sql()
    val data1 = spark.read.format("libsvm").load("F:\\spark-2.3.0-bin-hadoop2.7\\data\\mllib/sample_libsvm_data.txt").toDF("label","features")
    data1.show()

    }

}
