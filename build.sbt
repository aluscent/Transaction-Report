ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "3.5.1"

lazy val root = (project in file("."))
  .settings(
    Test / fork := true,
    Test / envVars ++= Map("POSTGRES_USER" -> "postgres", "POSTGRES_PASSWORD" -> "docker", "SPARK_LOCAL_IP" -> "127.0.0.1"),
    Test / parallelExecution := false,
    Test / javaOptions ++= Seq(
      "-Xmx2G",
      "-Dspark.sql.shuffle.partitions=4",
      "-XX:ActiveProcessorCount=2",
      "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
      "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED",
      "--add-opens=java.base/java.util=ALL-UNNAMED",
      "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED"
    ),
    name := "santander-task-harder",
    idePackagePrefix := Some("com.alitariverdy"),
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-streaming" % sparkVersion,
      "org.apache.spark" %% "spark-core" % sparkVersion,
      "org.apache.spark" %% "spark-sql" % sparkVersion,

      "com.typesafe" % "config" % "1.4.3",
      "org.scala-lang.modules" %% "scala-parser-combinators" % "2.4.0",

      "org.postgresql" % "postgresql" % "42.7.3",

      "com.dimafeng" %% "testcontainers-scala-postgresql" % "0.41.3" % Test,
      "com.dimafeng" %% "testcontainers-scala-scalatest" % "0.41.3" % Test,
      "org.scalatest" %% "scalatest" % "3.2.19" % Test,
      "org.jmockit" % "jmockit" % "1.49" % Test
    )
  )
