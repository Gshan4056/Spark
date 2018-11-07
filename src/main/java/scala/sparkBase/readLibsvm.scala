package scala.sparkBase

/**
  * Created by 91926 on 2018/8/9.
  */
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, udf}
object readLibsvm {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("readLibsvm")
      .getOrCreate()
    val data = spark.read.csv("data/libsvm.txt").toDF("value")
    data.show()

      val str = "128:51 129:159 130:253"
      val items = str.split(" ")
      items.foreach(item => println(item))
      val res = items.map { item =>
        val indexValue = item.split(':')
        val index = indexValue(0).toInt
        val value = indexValue(1).toDouble
        (index,value)
        //取数组尾部，对每一行进行下面的处理
//        val indexAndValue = item.split(':') //以:号分隔数据每个元素，存入indexAndValue
//      val index = indexAndValue(0).toInt - 1 // 将向量下标修改为从0开始
//      val value = indexAndValue(1).toDouble //将向量转换成Double
//        (index, value)
      }
      res.foreach(item => println(item))
  }
}
