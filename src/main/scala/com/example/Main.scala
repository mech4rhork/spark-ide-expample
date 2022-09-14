package com.example

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.lit

object Main extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    println("APP Name : " + spark.sparkContext.appName)
    println("Deploy Mode : " + spark.sparkContext.deployMode)
    println("Master : " + spark.sparkContext.master)

    val rangeDf = spark.range(1000 * 1000 * 1000)
    println("rangeDf.count() = " + rangeDf.count())

    val yoloDf = spark.range(1000).withColumn("yolo_col", lit("you only live once")).repartition(3)
    yoloDf.show(10, truncate = false)
    val outputFormats = List("csv", "orc", "parquet")
    outputFormats foreach { f =>
      yoloDf.write.format(f)
        .option("spark.sql.orc.impl", "native")
        .option("header", "false")
        .option("delimiter", ";")
        .mode(SaveMode.Overwrite)
        .save(s"hdfs://master:9000/tmp/yoloData/$f")
    }

//    val irisDf = spark.read.format("csv").option("header", "true").load("iris.csv")
//    irisDf.show(10, truncate = false)
  }
}
