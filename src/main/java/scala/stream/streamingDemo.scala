package scala.stream

/**
  * Created by 91926 on 2018/7/26.
  */
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
object streamingDemo {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local[2]").setAppName("NetworkWordCount")
    val ssc = new StreamingContext(conf, Seconds(5))
    val lines = ssc.socketTextStream("192.168.133.131", 9999)
    // Split each line into words
//    val words = lines.flatMap(_.split(" "))
    val uv = lines.map(line => (line.split(",")(1),1)).reduceByKey(_+_)
    import org.apache.spark.streaming.StreamingContext._ // not necessary since Spark 1.3
    // Count each word in each batch
//    val pairs = words.map(word => (word, 1))
//    val wordCounts = pairs.reduceByKey(_ + _)

    // Print the first ten elements of each RDD generated in this DStream to the console
    uv.print()
    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate

  }
}
