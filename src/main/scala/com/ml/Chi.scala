package com.ml

import com.ml.tokenizer.TokenizerUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 16/8/2016.
  */
object Chi {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/wangqi/skin.train")
    val s = sourceRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(_.split(",")).map(i => (i(0), i(1)))
      .mapPartitions(TokenizerUtil.formated)

    val labelCount = s.map(_._1).map((_, 1)).reduceByKey(_ + _)

    val termLabel = s.flatMap(i => i._2.map(j => ((i._1, j), 1.0))).reduceByKey(_ + _).collectAsMap()
    val common = s.map(_._2).flatMap(i => i.map(j => (j, 1.0))).reduceByKey(_ + _).collectAsMap()

    val result = for {
      term <- termLabel.keys.map(_._2)
      classes <- common.keys
    } yield {
      val all_num_term = termLabel.filter(i => i._1._2 == term).values.sum
      val all_num_cat = common(term)
      val a = termLabel.filter(i => i._1 == classes && i._2 == term).values.sum
      val b = common(classes) - a
      val c = all_num_term - a
      val d = all_num_cat - all_num_term - common(classes) + a
      val chi_score = if ((a + c) * (b + d) * (a + b) * (c + d) == 0)
        0
      else {
        val t = (a + c) * (b + d) * (a + b) * (c + d)
        Math.max(0, 1000 * Math.pow(a * d - b * c, 2) / t)
      }
      (term, chi_score)
    }
    result.take(10).foreach(println)
  }
}
