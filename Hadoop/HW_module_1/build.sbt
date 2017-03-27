import Dependencies.{providedDeps, _}

lazy val root = (project in file(".")).
  settings(
    name := "Yablokov_module1_hw1",
    inThisBuild(List(
      organization := "ru.yaal.epam.hadoop",
      scalaVersion := "2.11.8",
      version := "1"
    )),
    libraryDependencies ++= compileDeps union providedDeps.map(_ % "provided") union testDeps,
    javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:MaxPermSize=2048M", "-XX:+CMSClassUnloadingEnabled"),
    parallelExecution in Test := false,
    mainClass in assembly := Some("module1.hw1.Main"),
    assemblyJarName in assembly := "iablokov_module1_hw1.jar",
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in(Compile, run), runner in(Compile, run)),
    assemblyMergeStrategy in assembly := {
      case x if x.startsWith("META-INF") => MergeStrategy.discard
      case x if x.startsWith("""org\apache\commons""") => MergeStrategy.first
      case _ => MergeStrategy.deduplicate
    }
  )

lazy val mainRunner = project.in(file("mainRunner")).dependsOn(RootProject(file("."))).settings(
  libraryDependencies ++= compileDeps union providedDeps union testDeps
)