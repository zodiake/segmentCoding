package com.ml

import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{Vector, Vectors}

/**
  * Created by wangqi08 on 16/8/2016.
  */
class ChiHashingTF(val numberFeatures: Int, val dict: scala.collection.Map[String, (Int, Double)]) extends HashingTF(numberFeatures) {
  override def transform(document: Iterable[_]): Vector = {
    val termWeight = scala.collection.mutable.HashMap[Int, Double]()
    document.map(term => {
      if (dict.get(term.toString).isDefined) {
        val value = dict(term.toString)
        termWeight.put(value._1 - 1, value._2)
      }
    })
    Vectors.sparse(numFeatures, termWeight.toSeq)
  }
}
