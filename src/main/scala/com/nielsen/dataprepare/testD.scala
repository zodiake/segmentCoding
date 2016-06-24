package com.nielsen.dataprepare
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.charset.StandardCharsets
import org.apache.spark.rdd._
import org.apache.hadoop.fs.FileUtil
/**
 * @author Jason Wu
 *
 */
object testD {
  
  def main(args: Array[String]): Unit = {
    


    val itemfile = Source.fromFile("D:\\jd_test.raw").getLines().map(_.split(",\"")).filter(_.size == 6)
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

    val fi = itemfile.map( x => raw_split(x)).filter(! _.isEmpty).map(_.toList)
    fi.foreach(println)
    val fff = fi.map(x => (selectionitem(x).mkString(",") , x))
                  .toList
                  .groupBy(_._1) /*
                  .sortByKey()
                  .zipWithIndex
    //RDD[((String, Array[(String, List[String])]), Int)]              

    val item = fff.map(x => (x._1._1 , x._1._2.map(_._2(18).toFloat).sum/x._1._2.size, x._2))
                  .map(x => x._1 + "," + x._2.toString + "," + x._1.split(",").head + "%09.0f".format(x._3 * 1.0 + 1))
      
    val sales = fff.map(x => x._1._2.map(_._2)
                            .map(y => (List(y(14),y(15),y(16),y(17),y(18),y(19)).mkString(","), x._1._1.split(",").head + "%09.0f".format(x._2 * 1.0 + 1))))
                    .map(x => x.map(y => y._1 + "," + y._2))
                    .map(_.mkString("\n"))

    
    //item.saveAsTextFile(args(1))
    //sales.saveAsTextFile(args(2))
    */

  }
  
  def selectionitem(x:List[String]):List[String]={
    if(x.size > 16){
    List(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),
                  x(11),x(13),x(20))
    }else {
      List(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),
                  x(11),x(12),x(13))
    }
  }

  def raw_split(raw_data:Array[String]):Array[String] = {
    val fi0 = raw_data(0) + "," + raw_data(1).split("\",").head.replace(",","") + "," + raw_data(1).split("\",").reverse.head + "," + raw_data(2).replace("\"","") + "," + raw_data(3).replace("\"","")

    val fi4 = raw_data(4).split("\",")

    val fi4_1 = fi4(0).replace(",","")

    val fi4_2 = fi4(1)

    val fi5 = raw_data(5).replace("\",","*%$#").replace(",","").replace("*%$#",",")

    val fi = (fi0 + "," + fi4_1 + "," + fi4_2 + "," + fi5).split(",")

    if(fi.size ==21 || fi.size == 20){
      if(fi(18).size == 10){
        return fi(18).substring(0,7).replace("-","14") +: fi
      }else {
        return Array[String]()
      }
    }else {
      return Array[String]()
    }

  }

}
