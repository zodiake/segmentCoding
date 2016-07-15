package com.ml

import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Logistic {

  def formated(i: Iterator[(String, String)]) = {
    import collection.JavaConverters._
    val c = ArrayBuffer("袋", "瓶", "盒", "包", "克", "片", "罐", "支").asJava
    val analyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(c, true))
    for (s <- i)
      yield {
        val tokenStream = analyzer.tokenStream(null, new StringReader(s._2))
        val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
        tokenStream.reset()
        val list = new scala.collection.mutable.ListBuffer[String]()
        while (tokenStream.incrementToken()) {
          val term = charTermAttribute.toString()
          if (term.nonEmpty)
            list += term
        }
        tokenStream.close()
        (s._1, list.toList)
      }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[4]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("D:/wangqi/svm/test.csv")
      .map(i => i.split(","))
      .map(i => (i(1), i(0)))
      .map(i => if (i._1 == "non-audit") ("422", i._2) else i)
      .mapPartitions(formated)
      .randomSplit(Array(0.7, 0.3), seed = 11L)

    val trainRDD = sourceRDD(0)
    val testRDD = sourceRDD(1)

    val train = trainRDD.map(_._2)
    val test = testRDD.map(_._2)
    val trainLabel = trainRDD.map(_._1)
    val testLabel = testRDD.map(_._1)

    val dim = math.pow(2, 10).toInt
    val hashingTF = new HashingTF(dim)
    val tf = hashingTF.transform(train)
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val trainVector = tfidf.map(v => {
      v.asInstanceOf[SV]
    })

    val trainData = trainVector.zip(trainLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(162)
      .run(trainData)

    val tf1 = hashingTF.transform(test)
    val testTfIdf = idf.transform(tf1).map(v => v.asInstanceOf[SV])
    val testData = testTfIdf.zip(testLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val predictionAndLabels = testData.map { case LabeledPoint(label, features) =>
      val prediction = model.predict(features)
      (prediction, label)
    }

    val metrics = new MulticlassMetrics(predictionAndLabels)
    val precision = metrics.precision
    println("Precision = " + precision)
  }
}
