package lesson3.ioc.factory

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.StreamingContext

trait Factory {
  def sparkContext: SparkContext
  def streamingContext: StreamingContext
  def hiveContext: HiveContext
}
