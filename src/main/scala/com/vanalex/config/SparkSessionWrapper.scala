package com.vanalex.config

import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val sparkSession: SparkSession = {
    SparkSession.builder().master("local").appName("spark session").getOrCreate()
  }

}
