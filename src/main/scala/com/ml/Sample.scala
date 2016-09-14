import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet
import org.apache.spark.mllib.linalg.{SparseVector => SV}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 24/6/2016.
  */
object Sample {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:/winutil/")
    val conf = new SparkConf().setMaster("local").setAppName("Logistic")
    val sc = new SparkContext(conf)

    val sourceRDD = sc.textFile("d:/wangqi/abc.csv")
      .map(_.split(",")).map(i => (i(7), i(5)))
      .mapPartitions(i => {
        import collection.JavaConverters._
        val c = List("批发", "包邮", "进口", "天猫", "日本", "其他", "other", "小样", "i", "保湿", "正品", "代购").asJava
        val skinStopWords=List("正品","other","代购","韩国","现货","香港","小样","特价","台湾","美国","原装","i","直邮","新品","进口","系列","港代","国代","天然","其他","包邮","日本","柜正","专柜","玫瑰","男士","本代","法国","德国","新品","化妆").asJava
        val analyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(skinStopWords, false))
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
      })

    val commonTokens = sourceRDD.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).sortBy(-_._2)
    commonTokens.take(200).foreach(println)

    //keys.foreach(println)
    //println(sourceRDD.count())

    /*
    val s = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 == 1).keys.collect()
    val termsCount = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _).filter(_._2 != 1).keys.count()
    val re = sourceData.map(_._2).flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _)

    val termTrain = sc.textFile("d:/wangqi/sock.train").map(_.split(",")).map(array => array(1)).map(_.split("/")).collect().flatten
    val termTest = sc.textFile("d:/wangqi/sock.test").map(_.split(",")).map(array => array(1)).map(_.split("/")).collect.flatten
    val number = termTest.filter(i => termTrain.contains(i))
    println(termTrain.size)
    println(number.size)
    */
  }
}
