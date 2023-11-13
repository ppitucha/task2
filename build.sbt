ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.11"

lazy val root = (project in file("."))
  .settings(
    name := "TestData"
  )

val sparkVersion = "3.5.0"


libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.6"
libraryDependencies += "org.apache.hadoop" % "hadoop-client-api" % "3.3.6"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.17" % "test"
