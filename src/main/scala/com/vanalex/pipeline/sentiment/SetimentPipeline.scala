package com.vanalex.pipeline.sentiment

import com.johnsnowlabs.nlp.DocumentAssembler
import com.johnsnowlabs.nlp.annotator.{Lemmatizer, SentimentDetector, Tokenizer}
import com.johnsnowlabs.nlp.util.io.ReadAs
import com.vanalex.config.SparkSessionWrapper
import com.vanalex.pipeline.PipelineBuilder
import com.vanalex.util.ContentProvider

object SetimentPipeline extends SparkSessionWrapper{

    import sparkSession.implicits._

    def main(args: Array[String]): Unit = {

      val data = ContentProvider.sentimentPhrases.toDF("text")

      val documentAssembler = new DocumentAssembler()
        .setInputCol("text")
        .setOutputCol("document")

      val tokenizer = new Tokenizer()
        .setInputCols("document")
        .setOutputCol("token")

      val lemmatizer = new Lemmatizer()
        .setInputCols("token")
        .setOutputCol("lemma")
        .setDictionary("src/main/resources/lemmas_small.txt", "->", "\t")

      val sentimentDetector = new SentimentDetector()
        .setInputCols("lemma", "document")
        .setOutputCol("sentimentScore")
        .setDictionary("src/main/resources/default-sentiment-dict.txt", ",", ReadAs.TEXT)

      val pipeline = PipelineBuilder.build(Array(
        documentAssembler,
        tokenizer,
        lemmatizer,
        sentimentDetector,
      ))

      val result = pipeline.fit(data).transform(data)

      result.show(true)
    }
}
