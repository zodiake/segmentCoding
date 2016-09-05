package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 11/8/2016.
  */
object NaiveBayesTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NaiveBayes")
    val sc = new SparkContext(conf)

    val (trainData, validData) = LogisticValidate.prepareTFIDF(sc, TokenizerUtil.formatSet)

    val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = validData.map(p => (CategoryUtils.skinVector(model.predict(p.features).toInt), CategoryUtils.skinVector(p.label.toInt)))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / validData.count()

    //predictionAndLabel.saveAsTextFile("d:/wangqi/ml/naivebayes/sock")
    //predictionAndLabel.take(50).foreach(println)
    //predictionAndLabel.saveAsTextFile("d:/wangqi/ml/naivebayes/skin")
    println(accuracy)

  }
}
