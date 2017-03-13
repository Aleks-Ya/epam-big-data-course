package lesson3

import java.nio.file.Files

import lesson3.net.{FakeReceiver, NetworkInterfaceReceiver}
import lesson3.settings.service.HardcodeSettingsService
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object MainLocal {

  def main(args: Array[String]) {
    var sc: SparkContext = null
    var ssc: StreamingContext = null
    try {
      //      val hdfsDir = if (args.isEmpty) "hdfs://sandbox.hortonworks.com:8020/tmp/iablokov/spark/lesson2" else args(0)
      val conf = new SparkConf().setAppName("YablokovSpark3").setMaster("local[2]")
      //      sc = new SparkContext(conf)
      //      val hive = new HiveContext(sc)
      //      hive.sql("CREATE TABLE IN NOT EXISTS settings (ip STRING, type INT, value DOUBLE, period BIGINT);")
      //      val nullSettingsExists = hive.sql("SELECT ip FROM settings WHERE ip = 'NULL'").count() > 0
      //      if (!nullSettingsExists) {
      //        hive.sql("INSERT INTO settings(ip, type, value, period) VALUES ('NULL', 1, 0, 0);")
      //        hive.sql("INSERT INTO settings(ip, type, value, period) VALUES ('NULL', 2, 0, 0);")
      //      }

      val batchDuration = Seconds(5)
      val ssc = new StreamingContext(conf, batchDuration)
      sc = ssc.sparkContext
      ssc.checkpoint(Files.createTempDirectory("checkpoint_").toString)
      val stream = ssc.receiverStream(new FakeReceiver(2000))
      new TrafficAnalyzer(stream)

      ssc.start()
      ssc.awaitTermination()

      //      val sql = new SQLContext(sc)
      //      val hive = new HiveContext(sc)


    } finally {
      if (ssc != null) {
        ssc.stop()
      }
    }
  }

}