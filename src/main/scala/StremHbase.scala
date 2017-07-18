/**
  * Created by jacky on 7/18/17.
  */
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import SparkContext._
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.client.Put


object StremHbase {

  def main(args : Array[String]) {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("lxw1234.com")
    val sc = new SparkContext(sparkConf);
    var rdd1 = sc.makeRDD(Array(("ha",2),("aa",6),("Ca",7)))

    sc.hadoopConfiguration.set("hbase.zookeeper.quorum ","jacky")
    sc.hadoopConfiguration.set("hbase.zookeeper.property.clientPort","2181")
    sc.hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE,"hehe")
    var job = new Job(sc.hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])

    rdd1.map(
      x => {
        var put = new Put(Bytes.toBytes(x._1))
        put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
        (new ImmutableBytesWritable,put)
      }
    ).saveAsNewAPIHadoopDataset(job.getConfiguration)

    sc.stop()
  }


}
