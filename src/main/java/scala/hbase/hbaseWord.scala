package scala.hbase

/**
  * Created by 91926 on 2018/11/1.
  */
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{ConnectionFactory, Put}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableOutputFormat,TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object hbaseWord {
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("hbase"))
    val conf = HBaseConfiguration.create()
    var jobConf = new JobConf(conf)
    jobConf.set("hbase.zookeeper.quorum", "localhost:2181")
    //    jobConf.set("zookeeper.znode.parent", "/hbase")
    /*Write Hbase*/
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, "mytable")
    jobConf.setOutputFormat(classOf[TableOutputFormat])

    val rdd = sc.makeRDD(Array(1)).flatMap(_ => 0 to 100)
    rdd.map(x => {
      var put = new Put(Bytes.toBytes(x.toString))
      put.addColumn(Bytes.toBytes("professional"), Bytes.toBytes("salary"), Bytes.toBytes(x.toString))
      (new ImmutableBytesWritable, put)
    }).saveAsHadoopDataset(jobConf)

  }
}