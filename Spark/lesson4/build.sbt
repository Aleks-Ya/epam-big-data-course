import Dependencies.{providedDeps, _}

lazy val root = (project in file(".")).
  settings(
    name := "YablokovSpark4",
    inThisBuild(List(
      organization := "ru.yaal.epam.spark",
      scalaVersion := "2.11.8",
      version := "1"
    )),
    libraryDependencies ++= compileDeps union providedDeps.map(_ % "provided") union testDeps,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false,
    mainClass in assembly := Some("lesson4.Main"),
    assemblyJarName in assembly := "iablokov_spark_4.jar",
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run))
  )

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  libraryDependencies ++= compileDeps union providedDeps union testDeps
)