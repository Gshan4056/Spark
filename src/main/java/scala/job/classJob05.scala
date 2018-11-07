package scala.job


/**  * Created by MountainG on 2018/6/27.  */
import org.apache.spark.sql.SparkSession
object classJob05 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("classJob05")
      .getOrCreate()
    val userUseList = spark.read
      .option("header",false)
      .option("delimiter","\t")
      .csv("data/userUseList/*.gz")
      .toDF("date","userId","softName")
    val userData = spark.read
      .option("header",false)
      .option("delimiter","\t")
      .csv("data/userData/*.gz")
      .toDF("userId","date","label")
//      textFile("data/userData/*.gz")
    userUseList.createOrReplaceTempView("userUseList")
    userData.createOrReplaceTempView("userData")
    val subUserUseList = spark.sql("select * from userUseList where date==\"2016-04-01\" ")
    //数据转换
    val charcterData = spark.sql("select * from userUseList a join userData b on a.userId=b.userId")
    charcterData.show()
  }
}
