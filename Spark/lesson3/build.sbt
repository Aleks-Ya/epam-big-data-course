import Dependencies.{providedDeps, _}

lazy val root = (project in file(".")).
  settings(
    name := "YablokovSpark3",
    inThisBuild(List(
      organization := "ru.yaal.epam.spark",
      scalaVersion := "2.11.8",
      version := "1"
    )),
    libraryDependencies ++= compileDeps union providedDeps.map(_ % "provided") union testDeps,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false,
    mainClass in assembly := Some("lesson3.Main"),
    assemblyJarName in assembly := "iablokov_spark_3.jar",
    assemblyMergeStrategy in assembly := {
      case x if x.endsWith("MANIFEST.MF") => MergeStrategy.discard
      case x if x.endsWith("StaticUnknownPacketFactory.class") => MergeStrategy.first
      case PathList("org", "apache", xs@_*) => MergeStrategy.first
      case PathList("com", "google", xs@_*) => MergeStrategy.first
      case PathList("com", "esotericsoftware", xs@_*) => MergeStrategy.first
      case x if x.endsWith("rootdoc.txt") => MergeStrategy.first
      case x if x.endsWith("reference.conf") => MergeStrategy.first
      case x if x.endsWith("plugin.xml") => MergeStrategy.first
      case x if x.endsWith("parquet.thrift") => MergeStrategy.first
      case _ => MergeStrategy.deduplicate
    },
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
  )

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  libraryDependencies ++= compileDeps union providedDeps union testDeps
)