import sbt._
import Keys._
import com.typesafe.sbt.packager.docker._
import scala.sys.process._
import complete.DefaultParsers._

val scioVersion = "0.9.5"
val beamVersion = "2.24.0"
lazy val commonSettings = Def.settings(
  organization := "nl.vroste",
  // Semantic versioning http://semver.org/
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.13.3",
  scalacOptions ++= Seq("-target:jvm-1.8",
                        "-deprecation",
                        "-feature",
                        "-unchecked"),
  javacOptions ++= Seq("-source", "1.8", "-target", "1.8")
)

lazy val root: Project = project
  .in(file("."))
  .settings(commonSettings)
  .settings(assemblySettings)
  .settings(
    name := "scio-job",
    description := "scio job",
    publish / skip := true,
    run / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-core" % scioVersion,
      "com.spotify" %% "scio-extra" % scioVersion,
      "com.spotify" %% "scio-test" % scioVersion % Test,
      "org.apache.beam" % "beam-runners-direct-java" % beamVersion,
      "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % beamVersion,
      "org.slf4j" % "slf4j-simple" % "1.7.30"
    )
  )
  .enablePlugins(JavaAppPackaging)

lazy val repl: Project = project
  .in(file(".repl"))
  .settings(commonSettings)
  .settings(
    name := "repl",
    description := "Scio REPL for scio job",
    libraryDependencies ++= Seq(
      "com.spotify" %% "scio-repl" % scioVersion
    ),
    Compile / mainClass := Some("com.spotify.scio.repl.ScioShell"),
    publish / skip := true
  )
  .dependsOn(root)

lazy val gcpProject = settingKey[String]("GCP Project")
lazy val gcpRegion = settingKey[String]("GCP region")
lazy val createFlextTemplate = inputKey[Unit]("create DataflowFlexTemplate")
lazy val runFlextTemplate = inputKey[Unit]("run DataflowFlexTemplate")

lazy val assemblySettings = Def.settings(
  gcpProject := "",
  gcpRegion := "",
  assembly / test := {},
  assembly / assemblyJarName := "flex-wordcount.jar",
  assembly / assemblyMergeStrategy ~= { old =>
    {
      case s if s.endsWith(".properties") => MergeStrategy.filterDistinctLines
      case s if s.endsWith("public-suffix-list.txt") =>
        MergeStrategy.filterDistinctLines
      case s if s.endsWith(".class") => MergeStrategy.last
      case s if s.endsWith(".proto") => MergeStrategy.last
      case s if s.endsWith("reflection-config.json") => MergeStrategy.rename
      case s                         => old(s)
    }
  },
  Universal / mappings := {
    val fatJar = (Compile / assembly).value
    val filtered = (Universal / mappings).value.filter {
      case (_, name) => !name.endsWith(".jar")
    }
    filtered :+ (fatJar -> (s"lib/${fatJar.getName}"))
  },
  scriptClasspath := Seq((assembly / jarName).value),
  Docker / packageName := s"gcr.io/${gcpProject.value}/dataflow/templates/DataflowFlexTemplate",
  Docker / dockerCommands := Seq(
    Cmd(
      "FROM",
      "gcr.io/dataflow-templates-base/java11-template-launcher-base:latest"
    ),
    Cmd(
      "ENV",
      "FLEX_TEMPLATE_JAVA_MAIN_CLASS",
      (assembly / mainClass).value.getOrElse("")
    ),
    Cmd(
      "ENV",
      "FLEX_TEMPLATE_JAVA_CLASSPATH",
      s"/template/${(assembly / assemblyJarName).value}"
    ),
    ExecCmd(
      "COPY",
      s"1/opt/docker/lib/${(assembly / assemblyJarName).value}",
      "${FLEX_TEMPLATE_JAVA_CLASSPATH}"
    )
  ),
  createFlextTemplate := {
    val _ = (Docker / publish).value
    s"""gcloud beta dataflow DataflowFlexTemplate build 
          gs://${gcpProject.value}/dataflow/templates/${name.value}.json
          --image ${dockerAlias.value}
          --sdk-language JAVA
          --metadata-file metadata.json""" !
  },
  runFlextTemplate := {
    val parameters = spaceDelimited("<arg>").parsed
    s"""gcloud beta dataflow DataflowFlexTemplate run ${name.value}
    	--template-file-gcs-location gs://${gcpProject.value}/dataflow/templates/${name.value}.json
    	--region=${gcpRegion.value}
    	--parameters ${parameters.mkString(",")}""" !
  }
)
