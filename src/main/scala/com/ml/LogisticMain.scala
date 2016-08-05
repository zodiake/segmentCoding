package com.ml

import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.feature.{HashingTF, IDF}
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 29/7/2016.
  */
object LogisticMain {

  def formated(i: Iterator[(String, String)]) = {
    import collection.JavaConverters._
    val c = List("批发", "包邮", "进口", "休闲", "克", "特价", "包", "片", "天猫", "日本").asJava
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
        (s._1, list.toList)
      }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local[*]").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/wangqi/sock.txt")
    val data = sourceRDD.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
      .map(i => i.split(","))
      .map(i => (i(0), i(1)))
      .mapPartitions(formated)

    val train = data.map(_._2)
    val trainLabel = data.map(_._1)

    val dim = math.pow(2, 11).toInt
    val hashingTF = new HashingTF(dim)
    val tf = hashingTF.transform(train)
    val idf = new IDF().fit(tf)
    val tfidf = idf.transform(tf)
    val trainVector = tfidf.map(v => {
      v.asInstanceOf[SV]
    })
    val trainData = trainVector.zip(trainLabel).map(i => LabeledPoint(CategoryUtils.categoryToInt(i._2), i._1.toDense))

    val model = new LogisticRegressionWithLBFGS()
      .setNumClasses(52)
      .run(trainData)

    val prod = sc.textFile("d:/wangqi/hh")
      .map(i => i.split(","))
      .filter(i => i(5) == "零食/坚果/特产")
      .map(i => (i(15), i(10) + i(11) + i(12)))
      .mapPartitions(formated)

    val tf1 = hashingTF.transform(prod.map(_._2))
    val testTfIdf = idf.transform(tf1).map(v => v.asInstanceOf[SV]).map(i => i.toDense).zip(prod.map(_._1))

    val result = testTfIdf.map {
      case (features, id) =>
        val prediction = model.predict(features)
        (CategoryUtils.list(prediction.toInt), id)
    }

    result.saveAsTextFile("d:/wangqi/spark/logistic")
  }
}
