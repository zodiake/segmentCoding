package com.ml.tokenizer

import java.io.StringReader

import com.ml.analyzer.CJKWithoutNumberAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute
import org.apache.lucene.analysis.util.CharArraySet

/**
  * Created by wangqi08 on 18/8/2016.
  */
object TokenizerTest {
  def main(args: Array[String]): Unit = {
    import collection.JavaConverters._
    val str = "优卡/优卡/朱古力/橡皮糖/涂层/型/代/可可/巧克力/制品/多种/水果味/12/克/包"
    val c = List("批发", "包邮", "进口", "天猫", "日本", "盒","克","").asJava
    val analyzer = new CJKWithoutNumberAnalyzer(new CharArraySet(c, false))
    val tokenStream = analyzer.tokenStream(null, new StringReader(str))
    val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])
    tokenStream.reset()
    val list = new scala.collection.mutable.ListBuffer[String]()
    while (tokenStream.incrementToken()) {
      val term = charTermAttribute.toString()
      if (term.nonEmpty)
        list += term
    }
    tokenStream.close()
    println(list)
  }
}
