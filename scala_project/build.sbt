ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.3.7"

libraryDependencies += "com.lihaoyi" %% "ujson" % "1.4.0"

lazy val root = (project in file("."))
  .settings(
    name := "untitled"
  )
