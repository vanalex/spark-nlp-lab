package com.vanalex.pipeline

import com.vanalex.config.SparkSessionWrapper
import com.vanalex.util.ContentProvider
import org.apache.spark.ml.feature.NGram

object Ngram extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {
    val wordDataFrame = sparkSession.createDataFrame(ContentProvider.wordSequence)
    val ngram = new NGram().setN(2).setInputCol("words").setOutputCol("ngrams")
    val ngramDataFrame = ngram.transform(wordDataFrame)
    ngramDataFrame.select("ngrams").show(false)

    sparkSession.stop()
  }
}
