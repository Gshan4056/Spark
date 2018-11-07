package scala.stream
import kafka.serializer.{StringDecoder, StringEncoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka._
/**
  * Created by 91926 on 2018/10/26.
  */
object DirectStreamStyelTest {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      System.err.print("Usage: DirectStreamStyelTest <broker><topics>")
      System.exit(1)
    }
    val Array(broker,topics) = args
    val conf = new SparkConf().setMaster("local[2]").setAppName("DirectStreamStyelTest")
    val ssc = new StreamingContext(conf,Seconds(5))
    val topicSet = topics.split(",").toSet
    val kafkaParams = Map[String,String]("metadata.broker.list" -> broker)
    val messages = KafkaUtils.createDirectStream[String,String,StringDecoder,StringDecoder](
      ssc,kafkaParams,topicSet
    )
    val words = messages.map(_._2).flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1L))
      .reduceByKey(_+_)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
