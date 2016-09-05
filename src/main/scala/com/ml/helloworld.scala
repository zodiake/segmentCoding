package com.ml

/**
  * Created by wangnicole on 16/7/22.
  */

import org.apache.spark.{SparkConf, SparkContext}

object helloworld {
  def main(args: Array[String]) {
    //println("hello spark!")
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val dicts = sc.textFile("hdfs://localhost:9000/source/model/sock/model/sock.key")
    //fen ci zidian
    val dict_o = sc.textFile("hdfs://localhost:9000/source/DICT/NsDicts.txt")
    val lines = sc.textFile("hdfs://localhost:9000/source/model/sock/sock_test")
    var dicMap = Map[Char, List[String]]()
    val dic_key = dicts.map(x => x.split("\t")(0)).collect()
    //println(lines.count())
    //val line = lines.filter(x = > x.split(",").length = 16)
    val dict_all = dicts.++(dict_o)
    val pairs = dict_all.map(x => (x.split("\t")(0)(0), x.split("\t")(0))).groupByKey()
    val dicList = pairs.mapValues(x => x.toList)
    val sorted = dicList.map(x => (x._1, x._2.sortBy(- _.length)))
    //sorted.foreach{x => println(x._1,x._2)}
    //sorted.foreach(x => dicMap += (x._1 -> x._2))
    sorted.collect().foreach(x => dicMap += (x._1 -> x._2))
    println("dic:")
    //dicMap.foreach(x => println(x._1))
    val linee = lines.map(x => (x.split("\t")(1), x.split("\t")(1).replace("/", ""), x.split("\t")(0)))
    val result = linee.map(x => (x._1, getSplitWord(x._2, dicMap), x._3))
    //val result = linee.map(x => (getUseWord(x._1,dic_key),getUseWord(getSplitWord(x._2,dicMap),dic_key),x._3))
    result.saveAsTextFile("hdfs://localhost:9000/source/model/sock/afterresult3.txt")
    sorted.take(10).foreach(println)
    result.take(10).foreach(println)
    //println("all count:" + result.count())
    //val correct = result.map(x => if(x._1 == x._2) 1 else 0 )
    //println(correct.sum())

  }

  def getUseWord(lines: String, key: Array[String]): String = {
    var keyLine = ""
    val line = lines.split('/')
    line.foreach { x => if (key.contains(x)) if (keyLine.isEmpty) keyLine = x else keyLine = keyLine + '/' + x }
    //println(keyLine)
    return keyLine
  }

  def sortBylength(value: List[String]): List[String] = {
    val arrayNm = value.map(x => (x.length(), x))
    return arrayNm.sortBy(_._1).map(_._2).reverse
  }

  def getSplitWord(line: String, dic: Map[Char, List[String]]): String = {
    //val searchRdd = dic
    //println(line)
    var i = 0
    var lineR = ""
    while (i < line.length) {
      val c = line(i)
      val dicList = dic.get(c)
      if (dicList.isEmpty) i = i + 1
      else {
        var j = 0
        var k = 0
        var e = 0
        val lineList = dicList.get
        val lineArray = line.substring(i)
        //println(lineArray)
        while (k == 0 & !lineArray.isEmpty & e == 0) {
          //println(j)
          if (lineArray.startsWith(lineList(j).toString())) {
            k = 1
            i = i + lineList(j).length
            lineR = lineR + lineList(j) + '/'
          }
          else {
            if (j >= lineList.length - 1) e = 1
            else j = j + 1
          }
        }
        if (k == 0) i = i + 1
      }
    }
    //println(lineR)
    return lineR
  }
}