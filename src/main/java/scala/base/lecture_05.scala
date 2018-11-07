package scala.base

/**
  * Created by 91926 on 2018/7/11.
  */
import org.apache.spark.sql.SparkSession
object lecture_05 {
  def main(args: Array[String]): Unit = {
      val spark = SparkSession
        .builder()
        .master("local")
        .appName("leture_05")
        .getOrCreate()
      import spark.implicits._
      val userInstallArray = Array("userDate","userId","softName")
      val userInstallDF = spark.read.textFile("data/userUseList/000000_0","data/userUseList/000001_0")
      .map(x => x.split("\\s+"))
      .map(x => (x(0),x(1),x(2)))
      .toDF(userInstallArray:_*)
      userInstallDF.show()
      val userLabelArray = Array("userId","userDate","tagValue")
      val userLabelDF = spark.read.textFile("data/userData/000000_0","data/userData/000001_0")
        .map(x => x.split("\\s+"))
        .map(x => (x(0).trim,x(1),x(2).toDouble))
        .toDF(userLabelArray:_*)
      userLabelDF.show()
      userInstallDF.createOrReplaceTempView("userInstallDFTable")
      userLabelDF.createOrReplaceTempView("userLabelDFTable")

      val characterData = spark.sql("select a.userId,a.softName,(case when b.tagValue > 0.5 then cast(1 as double) else 0 end ) as label from userInstallDFTable a join userLabelDFTable b on a.userId=b.userId")
      characterData.show(100)

  }
}
