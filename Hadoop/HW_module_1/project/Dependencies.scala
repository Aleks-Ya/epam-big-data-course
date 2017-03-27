import sbt._

object Dependencies {
  private val hadoopVersion = "2.7.3"
  val compileDeps = Seq(
    "commons-beanutils" % "commons-beanutils-core" % "1.8.3",
    "org.apache.hadoop" % "hadoop-hdfs" % s"$hadoopVersion" exclude("commons-beanutils", "commons-beanutils-core"),
    "org.apache.hadoop" % "hadoop-common" % s"$hadoopVersion" exclude("commons-beanutils", "commons-beanutils-core")
  )

  val providedDeps: Seq[ModuleID] = Seq()

  val testDeps = Seq("org.scalatest" % "scalatest_2.11" % "3.0.1" % Test)
}