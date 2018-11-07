package scala.ml

/**
  * Created by 91926 on 2018/6/11.
  */
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.feature.{HashingTF, Tokenizer}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{Row, SparkSession}
object PipelineDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder
      .master("local")
      .appName("piplineDemo")
      .getOrCreate()
    // train Data
    val trainData = sparkSession.createDataFrame(Seq(
      (0L,"a b c d e spark",1.0),
      (1L,"b d",0.0),
      (2L,"spark f g h",1.0),
      (3L,"g h",0.0),
      (4L,"spark f g h",1.0),
      (5L,"spark f reduce h",1.0),
      (6L,"k l",0.0),
      (7L,"hadoop mapreduce",0.0))).toDF("id","text","label")
    //ML pipeline
    val tokenizer = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("words")
    val hashingTF = new HashingTF()
      .setNumFeatures(1000)
      .setInputCol(tokenizer.getOutputCol)
      .setOutputCol("features")
    val lr = new LogisticRegression()
        .setMaxIter(10)
        .setRegParam(0.001)
    val pipeline = new Pipeline()
      .setStages(Array(tokenizer,hashingTF,lr))
//    val model = pipeline.fit(trainData)
    // use paramGrid
    val paramGrid = new ParamGridBuilder()
      .addGrid(hashingTF.numFeatures,Array(10,100,1000))
      .addGrid(lr.regParam,Array(0.1,0.01))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new BinaryClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(2)
    val cvModel = cv.fit(trainData)
    //`save model
  //  cvModel.write.overwrite().save("/tmp/spark-logistic-regression-model")
 //   pipeline.write.overwrite().save("/tmp/unfit-lr-model")
  //  val sameModel = PipelineModel.load("/tmp/spark-logistic-regression-model")
    //test
    val testData = sparkSession.createDataFrame(Seq(
      (4L,"a b e spark"),
      (5L,"b e d"),
      (6L,"spark f g h"),
      (7L,"hadoop spark"))).toDF("id","text")
    val Predictions = cvModel.transform(testData)
    Predictions.select("id","text","probability","prediction")
      .collect()
      .foreach{
        case Row(id: Long,text: String,prob: Vector,prediction: Double) =>
          println(s"($id,$text) --> prob=$prob,prediction=$prediction")
      }
    //show bestModel
    val bestModel = cvModel.bestModel.asInstanceOf[PipelineModel]
    val lrModel = bestModel.stages(2).asInstanceOf[LogisticRegressionModel]
    println(lrModel.regParam)
    println(lrModel.numFeatures)
  }
}
