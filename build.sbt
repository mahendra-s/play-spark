name := """play-spark"""

version := "1.0"

lazy val root = (project in file(".")).enablePlugins(PlayScala)

scalaVersion := "2.11.7"

libraryDependencies ++= Seq(
  jdbc,
  cache,
  ws,
  "org.scalatestplus.play" %% "scalatestplus-play" % "1.5.1" % Test,
  "com.fasterxml.jackson.core" % "jackson-core" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.7",
  "com.fasterxml.jackson.core" % "jackson-annotations" % "2.8.7",
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.8.7",
  "org.apache.spark" % "spark-core_2.11" % "2.1.1",
  "org.webjars" %% "webjars-play" % "2.5.0-1",
  "org.webjars" % "bootstrap" % "3.3.6",
  "org.apache.spark" % "spark-sql_2.11" % "2.1.1"
)


libraryDependencies +=  "org.apache.spark" %% "spark-mllib" % "2.1.0"

libraryDependencies += "com.cloudera.sparkts" % "sparkts" % "0.4.0"

libraryDependencies += "org.codehaus.janino" % "janino" % "3.0.7"

libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }

