import scala.sys.process._

name := "provider"

ThisBuild / organization := "com.availity"
ThisBuild / scalaVersion := "2.12.13"
ThisBuild / useCoursier := false

// Assembly settings
assembly / assemblyJarName := s"${name.value}.jar"
assembly / assemblyMergeStrategy := {
  case PathList("module-info.class") => MergeStrategy.rename
  case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
  case PathList("META-INF", "INDEX.LIST") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.SF") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.DSA") => MergeStrategy.discard
  case PathList("META-INF", "DUMMY.RSA") => MergeStrategy.discard
    case x => MergeStrategy.first
}
  assembly / assemblyOption := (assembly / assemblyOption).value
.copy(includeScala = false)
  assembly / test := {}

  // Dependencies
  val sparkVersion = "3.1.1"

  libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.1",
      "org.apache.spark" %% "spark-sql" % "3.5.0",
      "com.github.mrpowers" %% "spark-daria" % "1.2.3",
      "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0" % Test,
      "org.scalatest" %% "scalatest" % "3.2.18" % Test)
