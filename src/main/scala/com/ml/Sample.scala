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

  def formated(i: Iterator[String]) = {
    import collection.JavaConverters._
    val c = ArrayBuffer("包邮", "正品", "进口", "批发", "现货", "皇冠", "特价", "新品").asJava
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

    val cateConfBroadCast = sc.broadcast(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/CATCONF.txt")).getLines().map(_.split(",")).map {
      case Array(a, b) => (a, b)
    }.toMap).value

    def prepareCateCode(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConfBroadCast.get(head.toUpperCase())
      if (cateTrans.isDefined) {
        s"${item.replace(head, cateTrans.get)},${head}"
      } else {
        s"${item}, "
      }
    }

    val sourceRDD = sc.textFile("D:/wangqi/testFile/train.csv")
    val tokens = sourceRDD.map(i => i.split(",")(3)).mapPartitions(formated)

    val category=sourceRDD.map(_.split(",")(0)).collect().distinct

    val common = tokens.flatMap(i => i.map(j => (j, 1))).reduceByKey(_ + _)
    println("-------------------most common----------------------")
    common.top(100)(Ordering.by[(String, Int), Int](_._2)).foreach(println)
    println("-------------------category-------------------------")
    category.foreach(println)
  }
}
