name := "Travel-Meta"

version := "0.1"

scalaVersion := "2.11.12"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.1.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.1.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive
libraryDependencies += "org.apache.spark" %% "spark-hive" % "2.1.3" % "provided"

// https://mvnrepository.com/artifact/org.apache.spark/spark-hive-thriftserver
libraryDependencies += "org.apache.spark" %% "spark-hive-thriftserver" % "2.1.3" % "provided"


/*libraryDependencies += "org.scalatest" % "scalatest_2.10" % "2.0" % "test"*/

// https://mvnrepository.com/artifact/org.scalatest/scalatest
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.0-SNAP7" % Test
