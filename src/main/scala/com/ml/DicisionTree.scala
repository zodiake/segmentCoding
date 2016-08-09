package com.ml

import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 9/8/2016.
  */
object DicisionTree {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val (trainData, validData) = LogisticValidate.prepareData(sc)

    val numClasses = 52
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 5
    val maxBins = 32
    val dicisionTreeModel = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val result = validData.map {
      case LabeledPoint(label, features) =>
        val prediction = dicisionTreeModel.predict(features)
        (prediction, label)
    }

    val metrics = new MulticlassMetrics(result)
    val precision = metrics.accuracy
    println("Precision = " + precision)
  }
}
