package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Logistic {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val (trainData, validData) = LogisticValidate.prepareWordCount(sc, TokenizerUtil.formatSet,13)
    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(52)
      .run(trainData)

    val predictionAndLabel = validData.map(p => (model.predict(p.features), p.label))
    //predictionAndLabel.saveAsTextFile("d:/wangqi/ml/logistic/sock"
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val precision = metrics.accuracy
    println("Precision = " + precision)
  }
}
