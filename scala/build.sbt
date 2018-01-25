lazy val root = (project in file("."))
  .settings(
    name         := "MyStuff",
    organization := "gov.cancer",
    scalaVersion := "2.11.8",
    version      := "0.1.0-SNAPSHOT",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1" % "provided",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1" % "provided"
  )
