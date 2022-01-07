package com.heap.nextgen

import org.apache.spark.sql.SparkSession
import org.joda.time.format.DateTimeFormat

import java.time.Instant
import scala.util.{Success, Try}

object ReadAvroWriteParquet extends App {

  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("read avro write parquet")
      //      .master("local[*]")   //for local testing
      .getOrCreate()

  val defaultInput =
    "s3://heap-platform-nextgen-data/3064244106/definedevents/spark/1633737600000/1637077513563/part-00000-2e3a4fec-6e64-49cd-8808-804274d00868-c000.avro"
  val inputPath: Seq[String] = Try(args(0)) match {
    case Success(input) => Seq(input)
    case _ =>
      println(s"WARNING: failed to read args(0) for inputPath, using $defaultInput")
      Seq(defaultInput)
  }

  val now = formatted(Instant.now().toEpochMilli)
  val defaultOutput = s"s3://heap-platform-nextgen-data/3064244106/definedevents/spark/parquet/$now"
  val outputPath: String = Try(args(1)) match {
    case Success(path) => path
    case _ =>
      println(s"WARNING: failed to read args(1) for outputPath, using $defaultOutput")
      defaultOutput
  }

  val df = spark.read.format("avro").load(inputPath: _*)
  df.write
    .parquet(outputPath)
  println(s"FINISHED writing parquet to $outputPath")
  spark.stop()

  def formatted(t: Long): String = {
    DateTimeFormat.forPattern("yyyyMMdd_HH:mm:ss").print(t)
  }
}
