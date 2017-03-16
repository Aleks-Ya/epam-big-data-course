package lesson3.hive

import lesson3.ioc.AppContext
import lesson3.settings.{Category, Settings}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode}
import org.slf4j.LoggerFactory

class HiveServiceImpl extends HiveService {
  private val log = LoggerFactory.getLogger(getClass)
  private val settings: List[Settings] =
    AppContext.hiveContext
      .table("settings")
      .map(row => new Settings(row.getString(0), Category.fromInt(row.getInt(1)), row.getDouble(2), row.getLong(3)))
      .collect.toList
  log.info("Settings loaded:\n" + settings)

  override def readSettings(): List[Settings] = {
    settings
  }

  override def updateTop3FastestIp(ips: List[String]): Unit = {
  }

  private val schema = StructType(
    StructField("time_stamp", StringType) ::
      StructField("ip", StringType) ::
      StructField("traffic_consumed", LongType) ::
      StructField("average_speed", DoubleType) ::
      Nil
  )

  override def saveHourStatistics(rdd: RDD[Row]): Unit = {
    log.info("Save hour statistics")
    val df = AppContext.hiveContext
      .createDataFrame(rdd, schema)

    val top3 = df.sort("average_speed")
      .take(3).reverse
      .map((row: Row) => row.getString(1) + " - " + row.getDouble(3))
      .mkString("\n")
    log.info(s"\nTop 3 by average speed:\n$top3\n")

    df
      .sort("average_speed")
      .write
      .mode(SaveMode.Append)
      .saveAsTable("statistics_by_hour")
  }
}
