import Dependencies._

lazy val root = (project in file(".")).
  settings(
    inThisBuild(List(
      organization := "ru.yaal.epam.spark",
      scalaVersion := "2.11.8",
      version      := "1"
    )),
    name := "Spark1",
    libraryDependencies ++= Seq(
		"org.scala-lang" % "scala-library" % "2.11.8",
		"org.apache.spark" % "spark-core_2.11" % "1.6.2",
		"org.scalatest" % "scalatest_2.11" % "3.0.1" % Test
	),
	javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
	parallelExecution in Test := false
  )
