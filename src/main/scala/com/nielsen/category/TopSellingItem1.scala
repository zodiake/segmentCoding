package main.java.com.nielsen.category

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.rdd.PairRDDFunctions
import scala.util.control.Breaks._
import scala.collection.mutable.ArrayBuffer
import com.nielsen.coding.codingUtil

object TopSellingItem1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf
    conf.setAppName("TopSellingItem")
    // conf.setMaster("local")
    val sc = new SparkContext(conf)

    val catogryAddressLst = sc.textFile(args(0)).collect()
    val salesAddresslst = sc.textFile(args(1)).collect()
    val tmpCategory = Array[String]()
    var cateRdd = sc.parallelize(tmpCategory)
    val tmpSales = Array[String]()
    var salesRdd = sc.parallelize(tmpSales)
    for (cateAdd <- catogryAddressLst) {
      cateRdd = sc.textFile(cateAdd) ++ cateRdd
    }
    for (salesAdd <- salesAddresslst) {
      salesRdd = sc.textFile(salesAdd) ++ salesRdd
    }
    /* cateRdd = sc.textFile("C://Users//Mirra//Desktop//Category")
     salesRdd = sc.textFile("C://Users//Mirra//Desktop//Sales")*/
    val salesText = salesRdd.map { x => x.split(",") }.map(x => (x(0), x(1)))
    val cateText = cateRdd.map { x => x.split(",", -1) }.map { x => (x(1), (x(4), x(0), x(2))) }
    //(itemId,((itemDesc,cateCode,brand),salesValue)) -- leftoutjoin
    //(cateCode,(itemId,itemDesc,brand,salesValue))

    val cateWithSales = cateText.leftOuterJoin(salesText).filter(!_._2._2.isEmpty)
    val totalText = cateWithSales.map { x => (x._2._1._2, (x._1, x._2._1._1, x._2._1._3, x._2._2.get.toDouble)) }.groupByKey()

    //各个category 的sales总和
    val totalSalesForCate = totalText.map { x => (x._1, caculateTotalSales(x._2) * 0.8) }

    //totalSalesForCate.collect().toList.foreach(println(_))

    //取出排在sales总和前80%的产品
    val topSellingItem = new PairRDDFunctions(totalText).leftOuterJoin(totalSalesForCate).map { x => (x._1, x._2._1.toList.sortBy(-_._4), x._2._2.get) }
      .map { x => get80PercentItem(x._1, x._2, x._3) }

    val result = topSellingItem.collect().map { x => x.mkString("\n") }

    val util = new codingUtil
    util.deleteExistPath(args(2))
    sc.parallelize(result).saveAsTextFile(args(2))


  }

  def caculateTotalSales(iterator: Iterable[(String, String, String, Double)]): Double = {
    var total = 0.0
    for (iter <- iterator) {
      total += iter._4
    }
    total
  }

  def get80PercentItem(cateCode: String, itemLst: List[(String, String, String, Double)], value: Double): ArrayBuffer[(String)] = {
    var selectedItemLst = ArrayBuffer[(String)]()
    var tempValue: Double = 0.0
    breakable(
      for (item <- itemLst) {
        tempValue += item._4
        if (tempValue < value) {
          selectedItemLst += (item._1 + "," + item._2 + "," + cateCode + "," + item._3)
        } else {
          break()
        }
      }
    )
    if (selectedItemLst.isEmpty) {
      val item = itemLst.apply(0)
      selectedItemLst += item._1 + "," + item._2 + "," + cateCode + "," + item._3
    }
    //println(selectedItemLst.mkString("\n"))
    selectedItemLst
  }

}