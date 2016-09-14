package com.ml.tokenizer

import java.io.StringReader

import com.ml.analyzer.{CJKWithoutNumberAnalyzer, StandardWithNoNumberAnalyzer}
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet

/**
  * Created by wangqi08 on 11/8/2016.
  */
object TokenizerUtil {
  private final val basedir = System.getProperty("SegDemo", "data")

  import collection.JavaConverters._

  def formated(i: Iterator[(String, String)]) = {
    import collection.JavaConverters._
    val c = List("批发", "包邮", "进口", "天猫", "日本", "其他", "other", "小样", "i", "保湿", "正品", "代购").asJava
    val skinStopWords=List("正品","other","代购","韩国","现货","香港","小样","特价","台湾","美国","原装","i","直邮","新品","进口","系列","港代","国代","天然","其他","包邮","日本","柜正","专柜","玫瑰","男士","本代","法国","德国","新品").asJava
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
  }

  def formatSet(i: Iterator[(String, String)]) = {
    import collection.JavaConverters._
    val c = List("批发", "包邮", "进口", "天猫", "日本", "其他", "克", "袋", "干", "包", "片", "装", "满", "斤", "盒", "元", "条", "粒", "个", "手", "件", "送", "邮", "的", "一", "份", "支", "三", "种", "块", "颗").asJava
    val skinStopWords=List("正品","other","代购","韩国","现货","香港","小样","特价","台湾","美国","原装","i","直邮","新品","进口","系列","港代","国代","天然","其他","包邮","日本","柜正","专柜","玫瑰","男士","本代","法国","德国","新品").asJava
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
        (s._1, list.toSet.toList)
      }
  }

  def standardFormatCombineCJK(i: Iterator[(String, String)]) = {
    val standardAnalyzer = new StandardWithNoNumberAnalyzer()
    val c = List("批发", "包邮", "进口", "天猫", "日本", "其他", "克", "袋", "干", "包", "片", "装", "满", "斤", "盒", "元", "条", "粒", "个", "手", "件", "送", "邮", "的", "一", "份", "支", "三", "种", "块", "颗").asJava
    val CJKAnalyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(c, false))
    for (s <- i)
      yield {
        val tokenStream = standardAnalyzer.tokenStream(null, new StringReader(s._2))
        val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])

        val tokenStream2 = CJKAnalyzer.tokenStream(null, new StringReader(s._2))
        val charTermAttribute2 = tokenStream.addAttribute(classOf[CharTermAttribute])

        tokenStream.reset()
        tokenStream2.reset()
        val list = new scala.collection.mutable.ListBuffer[String]()
        while (tokenStream.incrementToken()) {
          val term = charTermAttribute.toString()
          if (term.nonEmpty)
            list += term
        }
        while (tokenStream2.incrementToken()) {
          val term = charTermAttribute2.toString()
          if (term.nonEmpty)
            list += term
        }
        tokenStream.close()
        tokenStream2.close()
        (s._1, list.toList)
      }
  }

  def standardFormat(i: Iterator[(String, String)]) = {
    val analyzer = new StandardWithNoNumberAnalyzer()
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
  }

  def nlpTokenizer(i: Iterator[(String, String)]): Iterator[(String, List[String])] =
    for (s <- i) yield {
      (s._1, s._2.split("/").toList)
    }

  def stanfordSegment(line: Iterator[(String, String)]): Iterator[(String, List[String])] = {
    val c = List("批发", "包邮", "进口", "休闲", "克", "特价", "包", "片", "天猫", "日本").asJava

    for (i <- line) yield {
      (i._1, Nil)
    }
  }
}
