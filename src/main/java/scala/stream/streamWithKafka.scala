package scala.stream
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
/**
  * Created by 91926 on 2018/10/25.
  */
object streamWithKafka {

  def main(args: Array[String]): Unit = {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }
//    StreamingExamples.setStreamingLogLevels()
    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName("streamWithKafka")
    val ssc = new StreamingContext(conf,Seconds(5))
//    ssc.checkpoint("checkpoint")
    val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_+_)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
