import Dependencies._

lazy val root = (project in file(".")).
  settings(
    name := "YablokovSpark3",
    organization := "ru.yaal.epam.spark",
    scalaVersion := "2.11.8",
    version := "1",
    libraryDependencies ++= allDeps,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false,
    mainClass in assembly := Some("lesson3.Main"),
    assemblyJarName in assembly := "iablokov_spark_3.jar",
    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("MANIFEST.MF") => MergeStrategy.discard
      case x if x.endsWith("StaticUnknownPacketFactory.class") => MergeStrategy.first
      case _ => MergeStrategy.deduplicate
    }
  )
