import sbt._

object Dependencies {

  val compileDeps = Seq(
    "com.databricks" % "spark-csv_2.11" % "1.5.0",
    "org.pcap4j" % "pcap4j-packetfactory-static" % "1.7.0",
    "org.apache.kafka" % "kafka-clients" % "0.10.2.0"
  )

  val providedDeps = Seq(
    "org.apache.spark" % "spark-hive_2.11" % "1.6.2",
    "org.apache.spark" % "spark-streaming_2.11" % "1.6.2"
  )

  val testDeps = Seq("org.scalatest" % "scalatest_2.11" % "3.0.1" % Test)
}