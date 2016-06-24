package com.nielsen.dataprepare
import scala.io.Source
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import java.nio.charset.StandardCharsets
import org.apache.spark.rdd._
import org.apache.hadoop.fs.FileUtil

object HistoryDataSplit {

  
  def main(args: Array[String]): Unit = {
    
    val conf = new SparkConf()
    conf.setAppName("RawDataSplit")
    val sc = new SparkContext(conf)
    
    val itemfile = sc.textFile(args(0))
      
    val fi = itemfile.map(x => if(x.endsWith(",,")){x.reverse.tail.reverse + " ,"} else {x})
        .map(_.split(",").toList).filter(x => x.size == 19 ||x.size == 20 )
        .filter(_(18).size == 10)
        .map(x => x(18).substring(0,7).replace("-","14") :: x)

        
    val item = fi.map(x => (selectionitem(x) , x(18)))
                  .groupBy(_._1)
                  .map(x => (x._2.map(_._2.toFloat).sum/x._2.size).toString :: x._1.reverse)
                  .map(_.reverse)
                  .distinct
                  .zipWithIndex
                  .map(x => x._1.head + "%09.0f".format(x._2 * 1.0 + 1) :: x._1.reverse)
                  .map(_.reverse)         
                  //.foreach(x => println(x.mkString(",")))
    val item_t = item.map(x => (selectionitem(x).mkString(","),x.reverse.head))
      
      
    val sales = fi.map(x => (selectionitem(x).mkString(","),
                  List(x(14),x(15),x(16),x(17),x(18),x(19)).mkString(","))).join(item_t)
                  .map(x => x._2._1 + "," + x._2._2)
    
    item.map(x => x.mkString(",")).saveAsTextFile(args(1))
    sales.saveAsTextFile(args(2))
    
    sc.stop()
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
}
