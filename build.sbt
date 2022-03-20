import sbt.Keys.libraryDependencies

ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.12"

lazy val root = (project in file("."))
  .settings(
    name := "experimentscala",
    //libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "2.0.0",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.8",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.4.8",
      libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.11",
      libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.11" % "test",
    libraryDependencies += "org.scalatest" %% "scalatest-funsuite" % "3.2.11" % "test"
  )
