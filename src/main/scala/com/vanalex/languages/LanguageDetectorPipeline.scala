package com.vanalex.languages

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.LanguageDetectorDL
import com.vanalex.config.{Path, SparkSessionWrapper}
import com.vanalex.pipeline.PipelineBuilder.builder
import com.vanalex.util.Reader

object LanguageDetectorPipeline extends SparkSessionWrapper {

  def main(args: Array[String]): Unit = {
    val data = Reader.readTextFile(sparkSession, Path.languageText).toDF("text")
    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val languageDetector = LanguageDetectorDL.pretrained()
      .setInputCols("document")
      .setOutputCol("language")

    val pipeline = builder(Array(documentAssembler, languageDetector))
    val result = pipeline.fit(data).transform(data)

    result.select("language.result").show(false)
  }
}
