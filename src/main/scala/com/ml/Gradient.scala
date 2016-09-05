package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 9/8/2016.
  */
object Gradient {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val (trainData, validData) = LogisticValidate.prepareWordCount(sc,TokenizerUtil.formated)

    val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = validData.map(p => (model.predict(p.features), p.label))
    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / validData.count()

    println(accuracy)
  }
}
