package com.ml.train

import com.ml.CategoryUtils
import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 13/9/2016.
  */
object NaiveBayesTrain {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NaiveBayes")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/wangqi/skin.train")
    val sourceData = sourceRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(TokenizerUtil.formated)

    val rareTokens = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()

    val keys = sourceData.keys.collect()

    val validRDD = sc.textFile("d:/wangqi/abc.csv")

    val data2 = validRDD
      .map(i => i.split(","))
      .map(i => (i(5), i(7)))
      .filter(i => i._2 != "SKIN_BUNDLE")
      .filter(i => i._2 != "HAIR_BUNDLE")
      .map(i => (CategoryUtils.skinWord2IntMap(i._2).toString, i._1))
      .filter(i => keys.contains(i._1))
      .mapPartitions(TokenizerUtil.formated)

    val data = sourceData.map(_._2).filter(!rareTokens.contains(_))
    val label = sourceData.map(_._1)

    val validData = data2.map(_._2).filter(!rareTokens.contains(_))
    val validLabel = data2.map(_._1)

    val dim = math.pow(2, 14).toInt
    val hashingTF = new HashingTF(dim).setBinary(true)
    val tf = hashingTF.transform(data)
    val tf2 = hashingTF.transform(validData)

    val trainData = tf.zip(label).map(i => LabeledPoint(i._2.toInt, i._1.toDense))

    val v = tf2.zip(validLabel).map(i => LabeledPoint(i._2.toInt, i._1.toDense))

    val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "bernoulli")

    val predictionAndLabel = v.map(i => (model.predict(i.features), i.label))

    val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / data2.count()
    println(accuracy)
    //predictionAndLabel.filter(x => x._1 != x._2).take(100).foreach(println)
  }
}
