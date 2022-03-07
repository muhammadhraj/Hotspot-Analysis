import sbt.Keys.{libraryDependencies, scalaVersion, version}


lazy val root = (project in file(".")).
  settings(
    name := "CSE512-Hotspot-Analysis-Template",

    version := "0.1.0",

    scalaVersion := "2.11.11",

    organization  := "org.datasyslab",

    publishMavenStyle := true,

    mainClass := Some("cse512.Entrance")
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.2.0" % "compile",
  "org.apache.spark" %% "spark-sql" % "2.2.0" % "compile",
  "org.scalatest" %% "scalatest" % "2.2.4" % "test",
  "org.specs2" %% "specs2-core" % "2.4.16" % "test",
  "org.specs2" %% "specs2-junit" % "2.4.16" % "test"
)


// Deal with sbt deduplication during sbt assembly
// Source : https://stackoverflow.com/questions/25144484/sbt-assembly-deduplication-found-error

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
