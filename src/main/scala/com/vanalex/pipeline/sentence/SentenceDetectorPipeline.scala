package com.vanalex.pipeline.sentence

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.SentenceDetector
import com.vanalex.config.{Path, SparkSessionWrapper}
import com.vanalex.pipeline.PipelineBuilder
import com.vanalex.util.Reader

object SentenceDetectorPipeline extends SparkSessionWrapper{

  def main(args: Array[String]): Unit = {
    val data = Reader.readTextFile(sparkSession, Path.sentenceDetectorText).toDF("text")
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val pipeline = PipelineBuilder.build(Array(
      documentAssembler,
      sentence
    ))

    val result = pipeline.fit(data).transform(data)

    result.selectExpr("explode(sentence) as sentences").show(false)

  }
}