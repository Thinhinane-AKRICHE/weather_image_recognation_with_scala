ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.13.14"

lazy val root = (project in file("."))
  .settings(
    name := "microservice1",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "3.5.8" % Provided,
      "org.apache.spark" %% "spark-sql"  % "3.5.8" % Provided,
      "org.apache.logging.log4j" % "log4j-api" % "2.24.3",
      "org.apache.logging.log4j" % "log4j-core" % "2.24.3"
    )
  )