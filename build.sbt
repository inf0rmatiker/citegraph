ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.12.10"

// set the main class for 'sbt run'
Compile / run / mainClass := Some("org.citegraph.Application")


lazy val root = (project in file("."))
  .settings(
    name := "citegraph"
  )

val sparkVersion = "3.1.2"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion
)