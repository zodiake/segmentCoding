import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Sample {

  def format(i: Iterator[String]) = {
    import collection.JavaConverters._
    val c = ArrayBuffer("袋","瓶","盒","包","克","片","").asJava
    val analyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(c, true))
    for (s <- i)
      yield {
        val tokenStream = analyzer.tokenStream(null, new StringReader(s))
        val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
        tokenStream.reset()
        val list = new scala.collection.mutable.ListBuffer[String]()
        while (tokenStream.incrementToken()) {
          val term = charTermAttribute.toString()
          if (term.nonEmpty)
            list += term
        }
        tokenStream.close()
        list.toList
      }
  }

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("D:/wangqi/svm/test.csv")
    val tokens = sourceRDD.map(i => i.split(",")(0)).mapPartitions(format)

    val category=sourceRDD.map(_.split(",")(1)).collect().distinct

    val common = tokens.flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _)
    println("-------------------most common----------------------")
    common.top(100)(Ordering.by[(String, Int), Int](_._2)).foreach(println)
    println("-------------------category-------------------------")
    category.distinct.foreach(println)

  }
}
