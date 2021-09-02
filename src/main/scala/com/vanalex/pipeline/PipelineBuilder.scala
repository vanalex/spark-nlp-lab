package com.vanalex.pipeline

import org.apache.spark.ml.{Pipeline, PipelineStage}

object PipelineBuilder {

  def build(stages: Array[_ <: PipelineStage]): Pipeline ={
    new Pipeline().setStages(stages)
  }
}
