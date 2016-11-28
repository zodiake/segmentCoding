package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.HashingTF
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Logistic {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("/home/zodiake/Documents/item_master_sample/train_filter/tt")
    val data = sourceRDD.map(i => i.split(",")).filter(_.size == 2).map(i => (i(0), i(1)))

    val validate = sc.textFile("/home/zodiake/Documents/jd/jd_format_test/all_nc")
    val v = validate.map(i => i.split(",")).map(i => (i(0), i(1)))

    val label = data.map(_._1)
    val value = data.map(_._2.split(" ").toList)

    val vLable = v.map(_._1)
    val vValue = data.map(_._2.split(" ").toList)

    val hashingTF = new HashingTF()
    val tf = hashingTF.transform(value)
    val tf2 = hashingTF.transform(vValue)

    val trainData = tf.zip(label).map(i => LabeledPoint(CategoryUtils.skinCategoryToInt(i._2), i._1.toDense))
    val validate1 = tf2.zip(vLable).map(i => LabeledPoint(CategoryUtils.skinCategoryToInt(i._2), i._1.toDense))

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(2)
      .run(trainData)

    val predictionAndLabel = validate1.map(p => (model.predict(p.features), p.label))
    //predictionAndLabel.saveAsTextFile("d:/wangqi/ml/logistic/sock"
    val metrics = new MulticlassMetrics(predictionAndLabel)
    val precision = metrics.accuracy
    println("Precision = " + precision)
  }
}
