package com.ml

import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import com.ml.tokenizer.TokenizerUtil
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 8/8/2016.
  */
object LogisticValidate {

  def prepareDataWithBrand(sc: SparkContext, tokenizer: Iterator[(String, String)] => Iterator[(String, List[String])], n: Int = 14) = {
    val sourceRDD = sc.textFile("d:/wangqi/sock.train")
    val sourceData = sourceRDD
      .map(i => i.split(","))
      .map(i => (i(0), i(1), i(2), i(3)))
      .mapPartitions(i => {
        import collection.JavaConverters._
        val c = List("批发", "包邮", "进口", "天猫", "日本", "其他", "克", "袋", "干", "包", "片", "装", "满", "斤", "盒", "元", "条", "粒", "个", "手", "件", "送", "邮", "的", "一", "份", "支", "三", "种", "块", "颗").asJava
        val analyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(c, false))
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
            (s._1, list.toSet.toList)
          }
      })

    val keys = sourceData.keys.collect()

    val validRDD = sc.textFile("d:/wangqi/sock.test")
    val data2 = validRDD
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .filter(i => keys.contains(i._1))
      .mapPartitions(tokenizer)

    val data = sourceData.map(_._2)
    val label = sourceData.map(_._1)

    val validData = data2.map(_._2)
    val validLabel = data2.map(_._1)

    val dim = math.pow(2, n).toInt
    val hashingTF = new HashingTF(dim)
    val tf = hashingTF.transform(data)
    val idf = new IDF(1).fit(tf)
    val tfidf = idf.transform(tf)
    val trainVector = tfidf.map(v => {
      v.asInstanceOf[SV]
    })
    val trainData = trainVector.zip(label).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val tf1 = hashingTF.transform(validData)
    val testTfIdf = idf.transform(tf1).map(v => v.asInstanceOf[SV]).map(i => i.toDense)
    val testData = testTfIdf.zip(validLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    (trainData, testData)
  }

  def prepareTFIDF(sc: SparkContext, tokenizer: Iterator[(String, String)] => Iterator[(String, List[String])], n: Int = 14) = {
    val sourceRDD = sc.textFile("d:/wangqi/skin.train")
    val sourceData = sourceRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(tokenizer)

    val keys = sourceData.keys.collect()

    val s = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()

    val validRDD = sc.textFile("d:/wangqi/skin.test")
    val data2 = validRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .filter(i => keys.contains(i._1))
      .mapPartitions(tokenizer)

    val data = sourceData.map(_._2).filter(!s.contains(_))
    val label = sourceData.map(_._1)

    val validData = data2.map(_._2)
    val validLabel = data2.map(_._1)

    val dim = math.pow(2, n).toInt
    val hashingTF = new HashingTF(dim)
    val tf = hashingTF.transform(data)
    val idf = new IDF(1).fit(tf)
    val tfidf = idf.transform(tf)
    val trainVector = tfidf.map(v => {
      v.asInstanceOf[SV]
    })
    val trainData = trainVector.zip(label).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val tf1 = hashingTF.transform(validData)
    val testTfIdf = idf.transform(tf1).map(v => v.asInstanceOf[SV]).map(i => i.toDense)
    val testData = testTfIdf.zip(validLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    (trainData, testData)
  }

  def prepareWordCount(sc: SparkContext, tokenizer: Iterator[(String, String)] => Iterator[(String, List[String])], n: Int = 14) = {
    val sourceRDD = sc.textFile("d:/wangqi/sock.train")
    val sourceData = sourceRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(tokenizer)

    val s = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()

    val keys = sourceData.keys.collect()

    val validRDD = sc.textFile("d:/wangqi/sock.test")
    val data2 = validRDD
      .mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .filter(i => keys.contains(i._1))
      .filter(i => !s.contains(i._2))
      .mapPartitions(tokenizer)

    val data = sourceData.map(_._2).filter(!s.contains(_))
    val label = sourceData.map(_._1)

    val validData = data2.map(_._2)
    val validLabel = data2.map(_._1)

    val dim = math.pow(2, n).toInt
    val hashingTF = new HashingTF(dim)
    val tf = hashingTF.transform(data)
    val tf2 = hashingTF.transform(validData)
    /*
    val idf = new IDF(1).fit(tf)
    val tfidf = idf.transform(tf)
    val trainVector = tfidf.map(v => {
      v.asInstanceOf[SV]
    })
    */

    val trainData = tf.zip(label).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val testData = tf2.zip(validLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    (trainData, testData)

  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val (trainData, validData) = prepareWordCount(sc, TokenizerUtil.formated)

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(52)
      .run(trainData)

    val result = validData.map {
      case LabeledPoint(label, features) =>
        val prediction = model.predict(features)
        (prediction, label)
    }

    val metrics = new MulticlassMetrics(result)
    val precision = metrics.accuracy
    println("Precision = " + precision)

    result.filter(i => i._1 != i._2).saveAsTextFile("d:/wangqi/validate")
  }
}
