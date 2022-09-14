package com.example

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession = {
//    val masterUrl = "local[*]"
    val masterUrl = "spark://master:7077"
//    val masterUrl = "spark://master:8032"
    SparkSession.builder()
      .master(masterUrl)
      .appName("spark session")
      .config("spark.sql.orc.impl", "native")
      .getOrCreate()
  }

}