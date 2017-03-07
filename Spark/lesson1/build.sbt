import Dependencies._

lazy val root = (project in file(".")).
  settings(
    name := "YablokovSpark1",
    organization := "ru.yaal.epam.spark",
    scalaVersion := "2.11.8",
    version := "1",
    libraryDependencies ++= allDeps,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false,
    mainClass in assembly := Some("lesson1.Main"),
    assemblyJarName in assembly := "spark1.jar"
  )
