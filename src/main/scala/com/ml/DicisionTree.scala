package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.evaluation.MulticlassMetrics
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

    val (trainData, validData) = LogisticValidate.prepareWordCount(sc, TokenizerUtil.formatSet,13)

    val numClasses = 71
    val categoricalFeaturesInfo = Map[Int, Int]()
    val impurity = "gini"
    val maxDepth = 30
    val maxBins = 64
    val decisionTreeModel = DecisionTree.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
      impurity, maxDepth, maxBins)

    val predictionsAndLabels = validData.map(example =>
      (decisionTreeModel.predict(example.features), example.label)
    )
    val accuracy = new MulticlassMetrics(predictionsAndLabels).accuracy

    println("accuracy:" + accuracy)
  }
}
