package com.spark.coding

import com.nielsen.model.{IdAndKeyWordAndParentNo, Par}
import com.nielsen.packsize.PacksizeCoding
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
      if (cateTrans.isDefined)
        s"${item.replace(head, cateTrans.get)},${head}"
      else
        s"${item}, "

    }

    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00000")
      .map(prepareCateCode)
      .map(_.split(","))
      .filter(!_ (0).isEmpty)
      .map(i => ItemSegment(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13), i(8)))

    val categorySet = sc.broadcast(segConfig.map(i => (i(1), i(3))).groupBy(_._1).map(s => (s._1, s._2.map(_._2).toSet))).value
    val keywordsList = sc.broadcast(segConfig.map(i => ((i(1), i(3)), i)).groupBy(_._1).map(s => (s._1, s._2.map(_._2)))).value
    val segCode = sc.broadcast(segConfig.map(i => (i(0), i(10))).toMap).value

    val result = sourceRDD.filter(_.cateCode == "BIS").map { item =>

      def generalSegmentCoding(keywords: List[Array[String]]) = {
        val keyWordList = keywords.map(i => IdAndKeyWordAndParentNo(i(0), i(5)))
        implicit val alwaysTrue = (i: String) => true
        val b = keyWordList.flatMap(Par.parse(_)(item.brandDescription))
        b.sortBy(i => (i.index, i.par.length())) match {
          case Nil => s"${item.id},segno,UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
          case h :: t => s"${item.id},segno,${h.segmentId},${segCode(h.segmentId)},${item.perCode},${item.storeCode}"
        }
      }

      val segmentList = categorySet.getOrElse(item.cateCode, Set[String]()).toList
      item.cateCode match {
        case "FB" =>
          val specificSegment = "CAPACITY"
          val segList = segmentList.filter(_ != specificSegment)
          val general = for {
            segment <- segList
          } yield {
            val keywords = keywordsList((item.cateCode, segment))
            generalSegmentCoding(keywords)
          }
          val size = PacksizeCoding.getPackSize(item.brandDescription, "ML")
          val keywords = keywordsList((item.cateCode, specificSegment))
          val stageString = size match {
            case None => s"${item.id},segno,UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
            case Some(e) =>
              val t = keywords.filter(i => {
                val low = i(5).split("-").head.toFloat
                val high = i(5).split("-").reverse.head.toFloat
                if (e >= low && e <= high)
                  true
                else
                  false
              })
              if (t.size == 1)
                s"${item.id},342,${item.id},10,${item.perCode},${item.storeCode}"
              else
                s"${item.id},segno,UNKNOWN,UNKNOWN,${item.perCode},${item.storeCode}"
          }
          stageString :: general
        case "BIS" =>
          val segList = segmentList.filter(_ != "KRASEGMENT")
          val general = for {
            segment <- segList
          } yield {
            val keywords = keywordsList((item.cateCode, segment))
            generalSegmentCoding(keywords)
          }

          implicit val alwaysTrue = (i: String) => true
          val list = kra.flatMap(Par.parse(_)(item.brandDescription)).sortBy(_.segmentId)
          val kraSegment = list match {
            case Nil => s"${item.id},342,1460003,10,${item.perCode},${item.storeCode}"
            case h :: t => s"${item.id},342,${kraKey(h.segmentId)},10,${item.perCode},${item.storeCode}"
          }
          kraSegment :: general
        case "SP" =>
          val segList = segmentList.filter(_ != "LENGTH")
          val general = for {
            segment <- segList
          } yield {
            val keywords = keywordsList((item.cateCode, segment))
            generalSegmentCoding(keywords)
          }

          val keywords = keywordsList((item.cateCode, "LENGTH"))
        case "IMF" =>
          val specificSegment = "STAGE"
          val segList = segmentList.filter(_ != specificSegment)
          val general = for {
            segment <- segList
          } yield {
            val keywords = keywordsList((item.cateCode, segment))
            generalSegmentCoding(keywords)
          }

          val keywords = keywordsList((item.cateCode, specificSegment)).filter(i => i(4) != "不知道")
          val keyWordList = keywords.map(i => IdAndKeyWordAndParentNo(i(0), i(5), i.last))
          implicit val alwaysTrue = (i: String) => true
          val b = keyWordList.flatMap(Par.parse(_)(item.brandDescription))
          val c = BrandCoding.filterParentId(item.brandDescription, b)
          c.sortBy(i => (i.index, i.par.length())) match {
            case Nil => {
            }
            case h :: t => {
              s"${item.id},342,${h.segmentId},10,${item.perCode},${item.storeCode}"
            }
          }
      }
    }
    result.take(50).foreach(println)
  }
}
