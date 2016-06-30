package com.test

import java.io.StringReader

import org.apache.commons.lang.math.NumberUtils
import org.apache.lucene.analysis.custom.CustomAnalyzer
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute

/**
  * Created by wangqi08 on 28/6/2016.
  */
object LuceneTest {
  def main(args: Array[String]): Unit = {
    val analyzer = CustomAnalyzer.builder()
      .withTokenizer("standard")
      .build()
    val tokenStream = analyzer.tokenStream(null, new StringReader("【天猫超市】西班牙进口帕斯卡草莓味全脂酸奶125gx4/杯-天猫超市-天猫Tmall"))
    val charTermAttribute = tokenStream.addAttribute(classOf[CharTermAttribute])

    tokenStream.reset()
    val list = new scala.collection.mutable.ListBuffer[String]()
    while (tokenStream.incrementToken()) {
      val term = charTermAttribute.toString()
      list += term
    }
    val a = list.toList.filter(i => {
      if (i.length > 1)
        true
      else {
        if (NumberUtils.isDigits(i))
          true
        else
          false
      }
    })

    println(a)
    println(list)
  }
}
