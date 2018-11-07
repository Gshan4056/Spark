package scala.base

/**
  * Created by 91926 on 2018/6/14.
  */
import org.apache.spark.sql.SparkSession
object dataFrameDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("dataFrameDemo")
      .getOrCreate()
    import spark.implicits._
    val df = Seq(
      (1, "zhangyuhang", java.sql.Date.valueOf("2018-05-15")),
      (2, "zhangqiuyue", java.sql.Date.valueOf("2018-05-15"))
    ).toDF("id", "name", "created_time")
    df.createOrReplaceTempView("user")
    val namesDF  = spark.sql("SELECT * FROM user WHERE name like '%ang'")
    namesDF.show()
    val data = spark.read
      .option("head",false)
      .option("delimiter",",")
      .csv("data/people.txt")
      .toDF("name","age")
    data.createTempView("tempViewTable")
    println(data.show())
   val res = spark.sql("select * from tempViewTable where age > 19 and age <39")
    print(res.show())
    val dd = spark.read
      .textFile("data/people.txt")
    println(dd.show())

  }
}
