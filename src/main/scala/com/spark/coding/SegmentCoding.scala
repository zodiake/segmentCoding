package com.spark.coding

import com.nielsen.model.{IdAndKeyWord, Par, SegIdWithIndexAndSegName}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by wangqi08 on 22/6/2016.
  */
case class ItemSegment(cateCode: String, id: String, brandDescription: String, perCode: String, storeCode: String, bundle: String)

object SegmentCoding {

  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\winutil\\")
    val conf = new SparkConf().setMaster("local[2]").setAppName("My App")
    conf.setAppName("BrandCoding")
    val sc = new SparkContext(conf)
    val segmentConfigRDD = sc.textFile("D:/wangqi/testFile/SEGCONF").map(_.split(",")).cache

    val cateConf = sc.textFile("D:/wangqi/testFile/CATCONF").map(_.split(",")).map { case Array(a, b) => (a, b) }.collectAsMap
    val cateConfBroadCast = sc.broadcast(cateConf)
    def prepareCateCode(item: String): String = {
      val head = item.split(",")(0)
      val cateTrans = cateConfBroadCast.value.get(head.toUpperCase())
      if (cateTrans.isDefined) {
        s"${item.replace(head, cateTrans.get)},${head}"
      } else {
        s"${item}, "
      }
    }
    val sourceRDD = sc.textFile("D:/wangqi/testFile/part-00000")
      .map(prepareCateCode)
      .map(_.split(","))
      .map(i => ItemSegment(i(0), i(1), s"${i(2)} ${i(3)} ${i(4)}", s"${i(1)}".substring(0, 8), s"${i(1)}".substring(8, 13), i(8)))

    val configRDD = segmentConfigRDD.filter(i => i(3).toUpperCase == "SUBCATEGORY")
    val c = configRDD.map(i => (i.head, i(5).toUpperCase)).aggregate(List[(String, String)]())((u, t) => t :: u, (j, k) => j ++ k)
    val map = c.map(i => (i._1, i._2)).toMap

    def stageCoding = {
      val stageRDD = segmentConfigRDD
        .filter(i => i(3).toUpperCase == "STAGE" && i(1) == "IMF")
        .filter(i => i(4) == "不知道")
        .map(i => IdAndKeyWord(i.head, i(5).toUpperCase(), i.last))

      val stageMap = stageRDD.aggregate(List[IdAndKeyWord]())((u, t) => t :: u, (j, k) => j ++ k)

      sourceRDD.map(i => {
        val brandDesc = i.brandDescription
        def extractBrand(list: List[IdAndKeyWord], result: List[SegIdWithIndexAndSegName] = List[SegIdWithIndexAndSegName]()): List[SegIdWithIndexAndSegName] = {
          val res = list.map(Par.parse(_)(brandDesc)).filter(i => i.index != Nil)
          (list, result) match {
            case (Nil, _) => result
            case (_, h :: t) => result
            case _ => extractBrand(list.tail, res ++ result)
          }
        }

        val temp = extractBrand(stageMap).map(i => IdDescIndexParent(i.segmentId, i.segmentName, i.index, i.parentNo))

        val parentId = temp.map(i => i.parentId).toSet.filter { i => i == "-" }

        val filterParentList = for {
          i <- temp if (!parentId.contains(i.id))
        } yield i

        val b = filterParentList.to[ListBuffer]

        for (i <- b) {
          for (j <- b) {
            if (j.desc.indexOf(i.desc) > -1 && j.desc.size > i.desc.size) {
              b -= i
            } else
              b
          }
        }
        b.toList.sortBy(i => (i.index.head, i.desc.length())) match {
          case Nil => {
            s"${i.id},${123},TAOBAO_ZZZOTHER,TAOBAO_ZZZOTHER,${i.perCode},${i.storeCode}"
          }
          case h :: t => {
            s"${i.id},${123},${h.id},${1},${i.perCode},${i.storeCode}"
          }
        }
      })

    }

    val bundleRDD = sourceRDD.map(i => {
      val bundleId = getBundleSegId(i.bundle, c)
      s"${i.id},90062,${bundleId},${map(bundleId)},${i.perCode},${i.storeCode}"
    })
    bundleRDD.take(100).foreach(println)
  }

  def getBundleSegId(itemDesc: String, bundleSegConf: List[(String, String)]): String = {
    lazy val otherId = bundleSegConf.filter(_._2.toUpperCase().equals("OTHERS")).head._1
    val segConf = bundleSegConf.map { i => (i._1, i._2.split("/")) }.filter(_._2.length > 1)
    def go(list: List[(String, Array[String])]): String = list match {
      case Nil => otherId
      case h :: t => {
        if (h._2.length == itemDesc.split("/").length) {
          val min = h._2.map(i => itemDesc.indexOf(i)).min
          if (min >= 0) h._1 else go(t)
        } else {
          go(t)
        }
      }
    }
    go(segConf)
  }

}
