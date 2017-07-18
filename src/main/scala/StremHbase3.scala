/**
  * Created by jacky on 7/18/17.
  */

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.streaming.{Seconds, StreamingContext}


object StremHbase3 {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("HBaseTest").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    val conf = HBaseConfiguration.create()
    //设置zooKeeper集群地址，也可以通过将hbase-site.xml导入classpath，但是建议在程序里这样设置
    conf.set("hbase.zookeeper.quorum","jacky")
    //设置zookeeper连接端口，默认2181
    conf.set("hbase.zookeeper.property.clientPort", "2181")

    val tablename = "hehe"

    //初始化jobconf，TableOutputFormat必须是org.apache.hadoop.hbase.mapred包下的！
    val jobConf = new JobConf(conf)
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

   // val indataRDD = sc.makeRDD(Array("22,jack,15","12,Lily,16","23,mike,16"))
   val indataRDD = ssc.textFileStream("hdfs://jacky:9000/src").map(Sensor.parseSensor)

    indataRDD.foreachRDD { r =>
    r.map(Sensor.convertToPut)saveAsHadoopDataset(jobConf)
    }

//
//    val rdd = indataRDD.map(_.split(',')).map{arr=>{
//      /*一个Put对象就是一行记录，在构造方法中指定主键
//       * 所有插入的数据必须用org.apache.hadoop.hbase.util.Bytes.toBytes方法转换
//       * Put.add方法接收三个参数：列族，列名，数据
//       */
//      val put = new Put(Bytes.toBytes(arr(0).toString))
//      put.add(Bytes.toBytes("f1"),Bytes.toBytes("name"),Bytes.toBytes(arr(1).toString))
//      put.add(Bytes.toBytes("f1"),Bytes.toBytes("age"),Bytes.toBytes(arr(2).toString))
//      //转化成RDD[(ImmutableBytesWritable,Put)]类型才能调用saveAsHadoopDataset
//      (new ImmutableBytesWritable, put)
//    }}
//
//    rdd.saveAsHadoopDataset(jobConf)

    ssc.start()
    ssc.awaitTermination()
  }

  case class Sensor(rid:String,rv:String)
  object  Sensor {
    def parseSensor(str:String):Sensor= {
      val p = str.split(",")


      if (p(0).length == 2 ) {
        println("==================>" + p(0) + "  <<=== 风骚的分割线  ==>>  " + p(1))
      Sensor(p(0).toString, p(1).toString)
    }else{
      Sensor("0".toString, "0".toString)
      }
    }

    def convertToPut(sensor: Sensor): (ImmutableBytesWritable, Put) = {
      val rowkey = sensor.rid
      val put = new Put(Bytes.toBytes(rowkey.toString))
      // add to column family data, column  data values to put object
      put.add(Bytes.toBytes("f1"),Bytes.toBytes("hp"),Bytes.toBytes(sensor.rv.toString))

      return (new ImmutableBytesWritable(Bytes.toBytes(rowkey)), put)
    }

  }


}
