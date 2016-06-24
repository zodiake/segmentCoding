package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object CodeTestWithoutBroadCast {

  def main(args: Array[String]) {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    //val catlist = args(0).split(",")
    val raw_data_path = List("")
    //val des_data_path = List("aa_RESULT")

    val templist = Array[String]()

    var i = 1
    //add for bis
    var KRAFile = List[Array[String]]()
    //add end
    var cateCodeCombine = ""
    var segNoCombine = ""
    var ree = sc.parallelize(templist)
    //只获取需要catecode的segment config file
    def transCateCode(item: String, cateConf: List[Array[String]]): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConf.filter(x => x(0).equalsIgnoreCase(head))
      if (!cateTrans.isEmpty) {
        item.replace(head, cateTrans.map(_(1)).apply(0)) + "," + head
      } else {
        item + "," + " "
      }
    }
    val beginTime = System.nanoTime()
    val configFile = sc.textFile("C:/Users/wangqi08/Downloads/testFile/SEGCONF").map(_.split(",")).collect().toList
    val cateConf = sc.textFile("C:/Users/wangqi08/Downloads/testFile/CATCONF").map(_.split(",")).collect().toList //add for match bundedpack
    val testFile = sc.textFile("C:/Users/wangqi08/Downloads/testFile/sampleData.catcoded")
      .map(x => transCateCode(x, cateConf).split(","))
    val endTime = System.nanoTime()
    println(endTime - beginTime)
    testFile.collect().map(_.mkString(",")).foreach(println)
    //2.7
  }
}