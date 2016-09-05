package com.ml.fudan

import com.ml.LogisticValidate
import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 10/8/2016.
  */
object LibSvm {
  def main(args: Array[String]) = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val weight = sc.textFile("d:/wangqi/svm/test.key").map(i => i.split(",")).map(i => (i(0), (i(1), i(2)))).collectAsMap()
    val sourceData = sc.textFile("d:/wangqi/sock.train").mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(TokenizerUtil.formated)
    val result = sourceData.map {
      case (label, features) =>
        val f = features.filter(i => weight.keySet.contains(i)).map(i => weight(i)._1 + ":" + weight(i)._2)
        label + "\t" + f.mkString("\t")
    }
    result.saveAsTextFile("d:/wangqi/svm/libsvm")
  }
}
