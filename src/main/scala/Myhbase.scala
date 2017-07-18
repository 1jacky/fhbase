/**
  * Created by jacky on 7/16/17.
  */
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}



object Myhbase {

/**

  def main(args: Array[String]): Unit = {


//    val sparkConf = new SparkConf().setAppName("HdfsWordCount").setMaster("local[2]").setExecutorEnv("spark.executor.memory", "4g")
//
//    val ssc = new StreamingContext(sparkConf, Seconds(2))
//
//    val lines = ssc.textFileStream("hdfs://jacky:9000/src/")
//    val words = lines.flatMap(_.split(","))
//    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
//    wordCounts.print()
//    ssc.start()
//    ssc.awaitTermination()

    //spark-Stream
//    val sparkConf = new SparkConf().setAppName("HBaseStream")
    val sparkConf = new SparkConf().setAppName("test").setMaster("local[2]").set("spark.executor.memory", "4g")
    val ssc = new StreamingContext(sparkConf,Seconds(5))


    val conf = getHBaseConfig()

    val jobConf = new JobConf(conf,this.getClass)

    jobConf.set("mapreduce.job.output.key.class", classOf[Text].getName)
    jobConf.set("mapreduce.job.output.value.class", classOf[LongWritable].getName)
    jobConf.set("mapreduce.outputformat.class", classOf[TableOutputFormat[Text]].getName)
    jobConf.set("mapreduce.output.fileoutputformat.outputdir", "hdfs://jacky:9000/user")



    val connection = ConnectionFactory.createConnection(conf)

    val table = connection.getTable(TableName.valueOf("haha"))


    val admin = connection.getAdmin


    val sensorDStream = ssc.textFileStream("hdfs://jacky:9000/src").map(Sensor.parseSensor)
        //sensorDStream.print()
    sensorDStream.map(r => {
      var put = new Put(Bytes.toBytes(x._1))
      put.add(Bytes.toBytes("f1"), Bytes.toBytes("c1"), Bytes.toBytes(x._2))
      (new ImmutableBytesWritable,put)


    }
      //case (x,y) =>println(x)




//        InsertHbase(table,c)


      //rdd.map(Sensor.convert).saveAsHadoopDataset(jobConf)
      //rdd.map(Sensor.convert)

//      rdd.foreach(re => table.put(putRequest(re))
      //rdd.map(re => table.put(putRequest(re)))
    )
    ssc.start()
    ssc.awaitTermination()

//    val table = connection.getTable(TableName.valueOf("table1"))
//    val table = connection.getTable(TableName.valueOf("haha"))
//    val mput = new Put(Bytes.toBytes("row1"))
//    mput.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes("lishiz"))
//    table.put(mput)
//
//
//
//
//    //列出所有的表
//    val tbs = admin.listTables()
//
//    tbs.foreach(println)
//    //向已有的表中插入数据







  }

  def hh(s:String)={
    println(s+" shiwo<<<<<<<<")
  }
//hbase 链接配置
  def getHBaseConfig() : Configuration = {
    val conf= HBaseConfiguration.create()
    conf.set(TableOutputFormat.OUTPUT_TABLE, "haha")
    conf.set("hbase.zookeeper.quorum", "localhost")
    conf.set("hbase.zookeeper.property.clientPort","2181")
    conf
  }

  def putRequest(sensor: Sensor) = {
    val p = new Put(Bytes.toBytes(sensor.rid))
    p.add(Bytes.toBytes("word"), Bytes.toBytes("count"), Bytes.toBytes(sensor.rv))
  }

  //表添加格式
  def InsertHbase(table:Table,sensor: Sensor)={

    val key = sensor.rid
    val p = new Put(Bytes.toBytes(key))
    p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(sensor.rv))
    table.put(p)
    //table.put(p)
    // return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
  }


  case class Sensor(rid:String,rv:String)
  object  Sensor {
//    def parseSensor(str:String):Sensor={
//      val p = str.split(",")
//
//      println("=================================>"+p(0)+p(1))
//      Sensor(p(0).toString,p(1).toString)
//    }

        def parseSensor(str:String):(String,String)={
          val p = str.split(",")

          println("=================================>"+p(0)+p(1))
          (p(0).toString,p(1).toString)
        }



    def convert(sensor: Sensor)={
      val conf=HBaseConfiguration.create()
      val conn=ConnectionFactory.createConnection(conf)
      val userTable=TableName.valueOf("haha")
      val table=conn.getTable(userTable)
      val key = sensor.rid
      val p = new Put(Bytes.toBytes(key))
      p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(sensor.rv))
      table.put(p)
      //table.put(p)
     // return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
    }

//    def convert(sensor: Sensor):(ImmutableBytesWritable,Put)={
//      val key = sensor.rid
//      val p = new Put(Bytes.toBytes(key))
//      p.addColumn(Bytes.toBytes("cf"),Bytes.toBytes("name"),Bytes.toBytes(sensor.rv))
//      return (new ImmutableBytesWritable(Bytes.toBytes(key)), p)
//    }

  }


**/
}
