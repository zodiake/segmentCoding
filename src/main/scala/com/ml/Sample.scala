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

  def format(i: Iterator[String]) = {
    import collection.JavaConverters._
    val c = List("批发", "包邮", "进口", "休闲", "克", "特价", "包", "片", "天猫", "日本").asJava
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

    val sourceRDD = sc.textFile("d:/wangqi/hh")
    sourceRDD.take(1).foreach(println)
    sourceRDD
      .map(i => i.split(","))
      .map(i => (i(15), i(10) + i(11) + i(12)))
      .take(1).foreach(println)
  }
}
