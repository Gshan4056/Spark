package scala.ml

/**
  * Created by 91926 on 2018/6/21.
  */
import org.apache.spark.sql.SparkSession
object FPGrowthTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("FPGrowh")
      .getOrCreate()
//    val dataset = spark.createDataFrame(Seq(
//      "1 2 5".split(" "),
//      "1 2 3 5".split(" "),
//      "1 2".split(" ")
//    )map(Tuple1.apply))toDF("items")
//    dataset.show()

//    dataset.show()
//    val fpgrowth = new FPGrowth()
//      .setItemsCol("items")
//      .setMinConfidence(0.6)
//      .setMinSupport(0.5)
//    val model = fpgrowth.fit(dataset)
//    model.freqItemsets.show()
//    model.associationRules.show()
//    model.transform(dataset).show()
  }
}
