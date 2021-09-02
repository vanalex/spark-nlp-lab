package com.vanalex.util

import org.apache.spark.sql.{Dataset, SparkSession}

object Reader {

  def readTextFile(sparkSession: SparkSession, path: String): Dataset[_ >: String] ={
    sparkSession.read.textFile(path)
  }
}
