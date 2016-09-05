package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 9/8/2016.
  */
object RandomForestTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val (trainData, validData) = LogisticValidate.prepareWordCount(sc, TokenizerUtil.formatSet)

    val model = RandomForest.trainClassifier(trainData, 52, Map[Int, Int](), 200, "auto", "gini", 10, 100)
    val predictionsAndLabels = validData.map(example =>
      (model.predict(example.features), example.label)
    )
    val accuracy =
      new MulticlassMetrics(predictionsAndLabels).accuracy
    print(accuracy)
  }

}
