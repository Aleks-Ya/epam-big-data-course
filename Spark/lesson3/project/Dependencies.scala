import sbt._

object Dependencies {
	val allDeps = Seq(
    "org.scala-lang" % "scala-library" % "2.11.8" % Provided,
    "org.apache.spark" % "spark-sql_2.11" % "1.6.2" % Provided,
    "org.apache.spark" % "spark-core_2.11" % "1.6.2" % Provided,
    "org.apache.spark" % "spark-hive_2.11" % "1.6.2" % Provided,
    "org.apache.spark" % "spark-streaming_2.11" % "1.6.2",
    "com.databricks" % "spark-csv_2.11" % "1.5.0",
    "org.pcap4j" % "pcap4j-packetfactory-static" % "1.7.0",
    "org.scalatest" % "scalatest_2.11" % "3.0.1" % Test
	)
}