package lesson3

import lesson3.net.NetworkInterfaceReceiver
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Main {
  
  def main(args: Array[String]) {
    try {
//      val hdfsDir = if (args.isEmpty) "hdfs://sandbox.hortonworks.com:8020/tmp/iablokov/spark/lesson2" else args(0)
      val conf = new SparkConf().setAppName("YablokovSpark3").setMaster("local[2]")
//      sc = new SparkContext(conf)

      val batchDuration = Seconds(5)
      val ssc = new StreamingContext(conf, batchDuration)
      val packets = ssc.receiverStream(new NetworkInterfaceReceiver)
      packets.print()

      ssc.start()
      ssc.awaitTermination()

//      val sql = new SQLContext(sc)
//      val hive = new HiveContext(sc)


      
    } finally {
//      if (sc != null) {
//        sc.stop
//      }
    }
  }
  
}