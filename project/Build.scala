import sbt._
import Keys._
import sbtassembly.AssemblyPlugin.autoImport._

object BuildSettings {
  val buildOrganization = "jetlore"
  val buildScalaVersionPrefix = "2.10"
  val buildScalaVersion = buildScalaVersionPrefix + ".5"
  val buildVersion = "2.2.13-SNAPSHOT"

  val coreBuildSettings = Seq(
    organization := buildOrganization,
    scalaVersion := buildScalaVersion,
    version := buildVersion
  )

  val assemblySettings = Seq(
    test in assembly := {},
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
    assemblyMergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) => {
      case PathList("javax", "xml", xs@_*) => MergeStrategy.first
      case PathList("org", "slf4j", xs@_*) => MergeStrategy.first
      case PathList("org", "apache", "log4j", xs@_*) => MergeStrategy.first
      case PathList("javax", "annotation", xs@_*) => MergeStrategy.first
      case "library.properties" => MergeStrategy.discard
      case "asm-license.txt" => MergeStrategy.rename
      case s: String if s.startsWith("META-INF/maven/com.fasterxml.jackson.core") => MergeStrategy.filterDistinctLines
      case s: String if s.startsWith("META-INF/maven/com.sun.jersey") => MergeStrategy.filterDistinctLines
      case s: String if s.startsWith("META-INF/maven/org.codehaus.jettison") => MergeStrategy.filterDistinctLines
      case s: String if s.startsWith("META-INF/maven/org.slf4j") => MergeStrategy.filterDistinctLines
      case s: String if s.startsWith("META-INF/maven/ch.qos.logback") => MergeStrategy.filterDistinctLines
      case x => old(x)
    }
    },
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

  val buildSettings = BuildSettings.coreBuildSettings ++ BuildSettings.assemblySettings
}

object JetloreProject extends Build with Dependency {

  import BuildSettings._
  import CompileScope._
  import ProvidedScope._

  lazy val root = Project("spark-opentsdb-sink", file("."), settings = buildSettings).
    settings(libraryDependencies ++= Seq(
      spark_core_transitive % "provided",
      async_http_client, jackson_core)
    )
}

