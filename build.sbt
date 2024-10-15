ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.10"

lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.13" % "3.3.1",
  "org.apache.spark" % "spark-sql_2.13" % "3.3.1",
)