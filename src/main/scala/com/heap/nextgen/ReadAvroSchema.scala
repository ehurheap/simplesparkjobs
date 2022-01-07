package com.heap.nextgen

import org.apache.spark.sql.SparkSession

object ReadAvroSchema extends App {
  implicit val spark: SparkSession =
    SparkSession
      .builder()
      .appName("Liz read data")
      .master("local[4]")
      .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

  val inputPath = Seq(
    "file:///Users/lhurley/Downloads/part-00000-906ea1d6-4c9e-4997-afcd-c5a4ef474985-c000.avro"
  )

  val df = spark.read.format("avro").load(inputPath: _*)
  df.printSchema()
  df.schema.fieldNames foreach println

  df.show(100)
  spark.stop()

}
