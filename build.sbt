name := "Simple Spark Jobs"

version := "1.0"

scalaVersion := "2.11.12"
exportJars := true

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.8"

// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
libraryDependencies += "org.apache.spark" %% "spark-avro" % "2.4.8"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-core
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-core" % "2.13.1"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.core/jackson-databind
libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1"
// https://mvnrepository.com/artifact/com.fasterxml.jackson.module/jackson-module-scala
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-aws
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "3.3.1"
// https://mvnrepository.com/artifact/com.amazonaws/aws-java-sdk
libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.12.133"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.3.1"