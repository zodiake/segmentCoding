package com.nielsen.statics

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by zodiake on 17-3-9.
  */
object JDstatics {
  def main(args: Array[String]): Unit = {
    val Seq(par, source, target) = args.toSeq
    val conf = new SparkConf()
    if (par == "dev") {
      conf.setMaster("local[*]")
      conf.setAppName("statics")
    }
    val sc = new SparkContext(conf)
    val result = sc.textFile(source)
      .map(_.split("\t"))
      .map(i => (i(8), i(10).toDouble))
      .groupByKey()
      .map { i => (i._1, i._2.size, i._2.sum) }
      .map(i => (i._1, i._2, f"${i._3}%.2f"))
    result.repartition(1).saveAsTextFile(target)
  }
}
