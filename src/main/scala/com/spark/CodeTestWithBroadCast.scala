package com.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD.rddToPairRDDFunctions

object CodeTestWithBroadCast {
  /*
							 --args(0): catogory code(多个用逗号分割，全部用ALL)
							 --args(1): segment config file path
							 --args(2): input file path
							 --args(3): category config file path
							 --args(4): output file path
							 --args(5): coding 的类型（BRAND,SUBBRAND,PACKSIZE,SEGMENT）全部用ALL
							 --args(6): KRASegment config file path
							 * 
							 */
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
    val cateConf1 = sc.textFile("C:/Users/wangqi08/Downloads/testFile/CATCONF").map(_.split(",")).map { case Array(a, b) => (a, b) }.collectAsMap
    val cateConf1BroadCast = sc.broadcast(cateConf1)
    def transCateCode1(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConf1.get(head)
      if (cateTrans.isDefined) {
        item.replace(head, cateTrans.get) + "," + head
      } else {
        item + "," + " "
      }
    }
    val beginTime = System.nanoTime()
    val configFile = sc.textFile("C:/Users/wangqi08/Downloads/testFile/SEGCONF").map(_.split(",")).collect().toList
    val testFile1 = sc.textFile("C:/Users/wangqi08/Downloads/testFile/sampleCode.catcoded")
      .map(x => transCateCode1(x).split(","))
    //end
    val endTime = System.nanoTime()
    println(endTime - beginTime)
    //1255870369
  }
}
