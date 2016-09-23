package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.NaiveBayes
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 11/8/2016.
  */
object NaiveBayesTest {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("NaiveBayes")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/wangqi/sock.train")
    val sourceData = sourceRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(TokenizerUtil.formatSet)

    //val rareTokens = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()

    val keys = sourceData.keys.collect()

    val validRDD = sc.textFile("d:/wangqi/sock_valid.csv")
    val data2 = validRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(2), i(5)))
      .mapPartitions(TokenizerUtil.formatSet)

    val data = sourceData.map(_._2)
    val label = sourceData.map(_._1)

    val validData = data2.map(_._2)
    val validLabel = data2.map(_._1)

    val dim = math.pow(2, 14).toInt
    val hashingTF = new HashingTF(dim).setBinary(true)
    val tf = hashingTF.transform(data)
    val tf2 = hashingTF.transform(validData)

    val trainData = tf.zip(label).map(i => LabeledPoint(i._2.toInt, i._1.toDense))
    //val testData = tf2.zip(validLabel).map(i => LabeledPoint(i._2.toInt, i._1.toDense))

    val model = NaiveBayes.train(trainData, lambda = 1.0, modelType = "multinomial")

    val predictionAndLabel = validLabel.zip(model.predict(tf2))

    //val testData = tf2.zip(validLabel).map(i => LabeledPoint(i._2.toInt, i._1.toDense))

    //val predictionAndLabel = testData.map(p => (model.predict(p.features).toInt, p.label.toInt))

    //val accuracy = 1.0 * predictionAndLabel.filter(x => x._1 == x._2).count() / validData.count()
    //println(accuracy)

    predictionAndLabel.saveAsTextFile("d:/wangqi/ml/naivebayes/sock_923")

    //predictionAndLabel.filter(i => i._1 != i._2).foreach(println)
    /*
    val productData = sc.textFile("d:/wangqi/SKIN_06_DATA.csv").map(_.split(",")).map(i => (i(0), i(1))).mapPartitions(TokenizerUtil.formatSet)
    val productId = productData.map(_._1)
    val productFeatures = productData.map(_._2)

    val tfProduct = hashingTF.transform(productFeatures)
    val result = productId.zip(model.predict(tfProduct).map(_.toInt))
    result.saveAsTextFile("d:/wangqi/test/skin")
    */
  }
}
