package scala.stream

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.spark_project.dmg.pmml.True
import org.apache.spark.sql.SQLContext
/**
  * Created by 91926 on 2018/10/22.
  */
object toDataFrame {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("192.168.133.131", 9999)
    val blocks = List("Hive","Storm")
    val block1 = List("Hive")
    val blocksRDD = ssc.sparkContext.parallelize(blocks)
    val blockRDD = ssc.sparkContext.parallelize(block1)
//    blockRDD.collect()
    val words = lines.map(line => (line.split(" ")(4),line.split(" ")(3))).transform(rdd => {
      rdd.map(x => (x._1,x._2.toInt))
        .reduceByKey(_+_)
   })
    val addressAgeMap = lines.map(line => (line.split(" ")(4),line.split(" ")(3).toInt))
    val avgAgeResult = addressAgeMap.foreachRDD { rdd =>

      // Get the singleton instance of SQLContext
      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      // Convert RDD[String] to DataFrame
      val wordsDataFrame = rdd.toDF("word","count")

      // Register as table
      wordsDataFrame.registerTempTable("words")

      // Do word count on DataFrame using SQL and print it
      val wordCountsDataFrame =
        sqlContext.sql("select word, count(*) as total from words group by word")
      wordCountsDataFrame.show()
    }


    words.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
