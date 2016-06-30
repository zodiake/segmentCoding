package com.mlib

import java.io.StringReader

import org.apache.lucene.analysis.cjk.CJKAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Logistic {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00000").map(i => i.split(",")(4))
    val formatedRDD = sourceRDD.map(i => {
      val analyzer = new CJKAnalyzer()
      val tokenStream = analyzer.tokenStream(null, new StringReader(i))
      val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])

      tokenStream.reset()
      val list = new scala.collection.mutable.ListBuffer[String]()
      while (tokenStream.incrementToken()) {
        val term = charTermAttribute.toString()
        list += term
      }
      list.toList
    })


    val hashingTF = new HashingTF()
    val tf: RDD[Vector] = hashingTF.transform(formatedRDD)
    val idf = new IDF(minDocFreq = 2).fit(tf)
    val tfidf: RDD[Vector] = idf.transform(tf)

    val values = tfidf.map(i => i.toSparse.values.toList)

    //values.aggregate(List[Double]())((i, j) => i ++ j, (u, v) => u ++ v)

  }
}
