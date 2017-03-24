import sbt._

object Dependencies {

  val compileDeps = Seq(
    "org.apache.hadoop" % "hadoop-hdfs" % "2.7.3"
  )

  val providedDeps: Seq[ModuleID] = Seq()

  val testDeps = Seq("org.scalatest" % "scalatest_2.11" % "3.0.1" % Test)
}