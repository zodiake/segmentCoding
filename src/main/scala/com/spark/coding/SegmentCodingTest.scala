package com.spark.coding

import com.nielsen.model.{IdAndKeyWordAndParentNo, Par}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by wangqi08 on 23/6/2016.
  */
object SegmentCodingTest {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
    conf.setAppName("BrandCoding")
    val sc = new SparkContext(conf)
    val segConfig = scala.io.Source.fromInputStream(getClass.getResourceAsStream("/SEGCONF.txt")).getLines().map(_.split(","))
      .filter(i => i(3) != "BRAND" && !i(3).contains("SUBBRAND") && i(3) != "PACKSIZE" && i(3) != "PRICETIER" && i(3) != "CATEGORY").toList

    val kra = sc.broadcast(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/krasegment.txt")).getLines()
      .map(_.split(",")).toList
      .map(r => IdAndKeyWordAndParentNo(r(1), r(2)))
      .filter(_.keyWord != "其他")).value

    val kraKey = sc.broadcast(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/krasegment.txt")).getLines()
      .map(_.split(",")).toList
      .map(r => (r(1), r(0)))
      .toMap).value

    val cateConfBroadCast = sc.broadcast(scala.io.Source.fromInputStream(getClass.getResourceAsStream("/CATCONF.txt")).getLines().map(_.split(",")).map {
      case Array(a, b) => (a, b)
    }.toMap).value
    def prepareCateCode(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConfBroadCast.get(head.toUpperCase())
      if (cateTrans.isDefined) {
        s"${item.replace(head, cateTrans.get)},${head}"
      } else {
        s"${item}, "
      }
    }

    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00000")
      .map(prepareCateCode)
      .map(_.split(","))
      .filter(!_ (0).isEmpty)
      .map(i => ItemSegment(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13), i(8)))

    val categorySet = sc.broadcast(segConfig.map(i => (i(1), i(3))).groupBy(_._1).map(s => (s._1, s._2.map(_._2).toSet))).value

    val keywordsList = sc.broadcast(segConfig.map(i => ((i(1), i(3)), i)).groupBy(_._1).map(s => (s._1, s._2.map(_._2)))).value

    val segCode = sc.broadcast(segConfig.map(i => (i(1), i(2))).toMap).value

    val result = sourceRDD.filter(_.cateCode == "BIS").map { item =>
      val segmentList = categorySet.getOrElse(item.cateCode, Set[String]()).toList
      item.cateCode match {
        case "BIS" => {
          val segList = segmentList.filter(_ != "KRASEGMENT")
          val general = for {
            segment <- segList
          } yield {
            val keywords = keywordsList((item.cateCode, segment))
            val keyWordList = keywords.map(i => IdAndKeyWordAndParentNo(i(0), i(5)))
            val b = keyWordList.map(Par.parse(_)(item.brandDescription)).flatten
            b.sortBy(i => (i.index, i.par.length())) match {
              case Nil => s"${item.id},segno,"
              case h :: t => s"${item.id},segno,${h.segmentId},segcode,${item.perCode},${item.storeCode}"
            }
          }

          val list = kra.map(Par.parse(_)(item.brandDescription)).flatten.sortBy(_.segmentId)
          val kraSegment = list match {
            case Nil => s"${item.id},342,1460003,10,${item.perCode},${item.storeCode}"
            case h :: t => s"${item.id},342,${kraKey(h.segmentId)},10,${item.perCode},${item.storeCode}"
          }
          kraSegment :: general
        }
      }
    }
    result.take(50).foreach(println)
  }
}
