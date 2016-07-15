package com.nielsen.dataprepare

import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author Jason Wu
  *
  */
object RawDataSplit {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\");
    val conf = new SparkConf().setMaster("local").setAppName("RawDataSplit")
    val sc = new SparkContext(conf)

    //添加批次的改动
    var batchNum = "12"
    val source = args(0)
    val itemTarget = args(1)
    val salesTarget = args(2)

    val itemfile = sc.textFile(source).map(_.split(",\"")).filter(_.size == 6)
      .filter(x => x(4).split("\",").size == 2)
    /*
    val fi0 = itemfile.map(x => x(0) + "," + x(1).split("\",").head.replace(",","") + ","
                  + x(1).split("\",").reverse.head + "," 
                  + x(2).replace("\"","") + "," + x(3).replace("\"",""))
    
    
    val fi4 = itemfile.map(_(4).split("\","))//.filter(_.size == 2)
          .map(x => x(0).replace(",","")+ "," + x(1))//.foreach(println)
    
    val fi5 = itemfile.map(_(5).replace("\",","*%$#").replace(",","").replace("*%$#",","))//.foreach(println)
        
    val fi = fi0.zip(fi4).map(x => x._1 + "," + x._2).zip(fi5).map(x => x._1 + "," + x._2)
        .map(x => if(x.endsWith(",")){x + " "} else {x})
        .map(_.split(",").toList).filter(x => x.size == 21 ||x.size == 20 )
        .filter(_(18).size == 10)
        .map(x => x(18).substring(0,7).replace("-","14") :: x)
        .filter(_(0) == "20151402")
    */

    val fi = itemfile.map(x => raw_split(x)).filter(!_.isEmpty).map(_.toList)

    try {
      val fff = fi.map(x => (selectionitem(x).mkString(","), x))
        .groupBy(_._1)
        .sortByKey()
        .zipWithIndex
      //RDD[((String, Array[(String, List[String])]), Int)]
      //.filter(x=>filterInvalidStr(x._2(18)))
      val item = fff.map(x => (x._1._1, x._1._2.map(_._2(18).toFloat).sum / x._1._2.size, x._2))
        .map(x => x._1 + "," + x._2.toString + "," + x._1.split(",").head +
          (if (x._1.split(",")(4).equalsIgnoreCase("B")) "10020"
          else if (x._1.split(",")(4).equalsIgnoreCase("C")) "10010"
          else if (x._1.split(",")(4).equalsIgnoreCase("S")) "10022"
          else x._1.split(",")(4))
          + "%09.0f".format(x._3 * 1.0 + 1) + batchNum)

      //match the new format of new item             
      /* val item = fff
      //.filter(x=>x._1._1.split(",").length==14)
                  .map(x => (x._1._1.split(",") , x._1._2.map(_._2(18).toFloat).sum/x._1._2.size, x._2))
                  .map(x => x._1.head+","+x._1(1)+","+x._1(3)+"," 
                      + ( if(x._1(4).equals("10010")) "C" 
                            else if (x._1(4).equals("10020")) "B" 
                            else x._1(4)
                            )
                      +","+x._1(5)+","+x._1(6)+","+x._1(7)+","+x._1(8)+","+x._1(9)+","+x._1(10)+","
                      + x._1(11)+","+x._1(12)+","+ ""+ "," + x._1.head 
                      + (if(x._1(4).equalsIgnoreCase("B")) "10020"
                           else if(x._1(4).equalsIgnoreCase("C")) "10010"
                           else x._1(4)
                      ) 
                      + "%09.0f".format(x._3 * 1.0 + 1)+","
                      + (if(x._1(13).equalsIgnoreCase("$#*")) ""
                           else x._1(13)
                      )
                      +","+""+","+""+","+x._1(2)+","
                      +x._2.toString)*/

      val sales = fff.map(x => x._1._2.map(_._2)
        .map(y => (List(y(14).trim(), y(15).trim(), y(16).trim(), y(17).trim(), y(18).trim(), (y(19) + " 00:00:00")).mkString(","), x._1._1.split(",").head +
          (if (x._1._1.split(",")(4).equalsIgnoreCase("B")) "10020"
          else if (x._1._1.split(",")(4).equalsIgnoreCase("C")) "10010"
          else if (x._1._1.split(",")(4).equalsIgnoreCase("S")) "10022"
          else x._1._1.split(",")(4)) + "%09.0f".format(x._2 * 1.0 + 1) + batchNum)))
        .map(x => x.map(y => y._1 + "," + y._2 + "," + y._2.substring(0, 8) + "," + y._2.substring(8, 13)))
        .map(_.mkString("\n"))

      //match the new format of new sales
      /* val sales = fff.map(x => x._1._2.map(_._2)
                              .map(y => (List(y(14),y(15),y(16),y(17),y(18)).mkString(","), x._1._1.split(",").head 
                                  + (if(y(4).equalsIgnoreCase("B")) "10020"
                                     else if(y(4).equalsIgnoreCase("C")) "10010"
                                     else y(4)
                                     )  
                                  + "%09.0f".format(x._2 * 1.0 + 1),y(19))))
                      .map(x => x.map(y => y._2+","+ y._3 + "," + y._1 + "," + y._2.substring(0, 8) +","+""+","+""+","+""))
                      .map(_.mkString("\n")) */
      //deleteExistPath(itemTarget)
      //deleteExistPath(salesTarget)
      //item.saveAsTextFile(itemTarget)
      //sales.saveAsTextFile(salesTarget)
      item.take(10).foreach(println)
    } catch {
      case t: Exception => t.printStackTrace()
    }

    sc.stop()
  }

  def deleteExistPath(pathRaw: String) {
    val outPut = new Path(pathRaw)
    val hdfs = FileSystem.get(URI.create(pathRaw), new Configuration())
    if (hdfs.exists(outPut)) hdfs.delete(outPut, true)
  }

  def selectionitem(x: List[String]): List[String] = {
    if (x.size > 16) {
      List(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10),
        x(11), x(13), x(20))
    } else {
      List(x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10),
        x(11), x(12), x(13))
    }
  }

  def raw_split(raw_data: Array[String]): Array[String] = {
    val fi0 = raw_data(0) + "," + raw_data(1).split("\",").head.replace(",", "") + "," + raw_data(1).split("\",").reverse.head + "," + raw_data(2).replace("\"", "") + "," + raw_data(3).replace("\"", "")

    val fi4 = raw_data(4).split("\",")

    val fi4_1 = fi4(0).replace(",", "")

    val fi4_2 = fi4(1)

    val fi5 = raw_data(5).
      replace("\",", "*%$#").replace(",", "").replace("*%$#", ",")

    val fi = (fi0 + "," + fi4_1 + "," + fi4_2 + "," + fi5).split(",", -1)

    if (fi.size == 21 || fi.size == 20) {
      if (fi(18).size == 10) {
        return fi(18).substring(0, 7).replace("-", "14") +: fi
      } else {
        return Array[String]()
      }
    } else {
      return Array[String]()
    }

  }

  def filterInvalidStr(str: String): Boolean = {
    val charLst = str.toCharArray().toList
    val result = charLst.filter(x => x != '.').map { x => x.isDigit }
    if (result.contains(false)) {
      return false
    } else {
      return true
    }
  }
}
