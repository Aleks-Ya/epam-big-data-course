package lesson4

import org.apache.spark.sql.SparkSession

object SparkHelper {
  lazy val ss: SparkSession = initSparkSession

  private def initSparkSession: SparkSession = {
    //TODO use >1 cores
    val builder = SparkSession.builder().appName("Iablokov Lesson 4").master("local[1]")

    val logDir = sys.env.get("SPARK_HISTORY_FS_LOG_DIRECTORY")
    if (logDir.isDefined) {
      builder
        .config("spark.eventLog.enabled", "true")
        .config("spark.eventLog.dir", logDir.get)
      println("Set event log dir: " + logDir.get)
    }
    val ss = builder.getOrCreate()
    ss
  }

}
